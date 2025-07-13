#[macro_use]
extern crate matches;

use rand::{Rng, distr::Alphanumeric, rng};
use std::{
    collections::HashSet,
    sync::{Arc, Once},
};
use tokio_nsq::*;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        env_logger::builder().is_test(true).try_init().ok();
        rustls::crypto::ring::default_provider()
            .install_default()
            .unwrap();
    });
}

fn random_topic() -> Arc<NSQTopic> {
    let mut rng = rng();
    let name: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();

    NSQTopic::new(name).unwrap()
}

async fn cycle_messages(
    topic: Arc<NSQTopic>,
    producer: &mut NSQProducer,
    consumer: &mut NSQConsumer,
) {
    let n: u8 = 10;

    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Healthy());

    for x in 0..n {
        producer
            .publish(&topic, x.to_string().as_bytes().to_vec())
            .await
            .unwrap();
    }

    for _ in 0..n {
        assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());
    }

    for x in 0..n {
        let message = consumer.consume_filtered().await.unwrap();

        assert_eq!(message.attempt, 1);
        assert_eq!(message.body, x.to_string().as_bytes().to_vec());

        message.finish().await;
    }
}

async fn run_message_tests(
    topic: Arc<NSQTopic>,
    mut producer: NSQProducer,
    mut consumer: NSQConsumer,
) {
    // Several basic PUB
    cycle_messages(topic.clone(), &mut producer, &mut consumer).await;

    // Large PUB
    let mut large = Vec::new();
    large.resize(1024 * 1024, 0);

    producer.publish(&topic, large.clone()).await.unwrap();
    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, large);
    message.finish().await;

    // MPUB
    producer
        .publish_multiple(&topic, vec![b"alice".to_vec(), b"bob".to_vec()])
        .await
        .unwrap();
    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"alice".to_vec());
    message.finish().await;

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"bob".to_vec());
    message.finish().await;

    // DPUB
    producer
        .publish_deferred(&topic, b"hello".to_vec(), 1)
        .await
        .unwrap();
    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"hello".to_vec());
    message.finish().await;
}

fn make_default() -> (Arc<NSQTopic>, NSQProducer, NSQConsumer) {
    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4150").build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4150".to_string(),
        ]))
        .build();

    (topic, producer, consumer)
}

#[tokio::test]
async fn direct_connection_basic() {
    init();

    let (topic, producer, consumer) = make_default();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_auth() {
    init();

    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4151")
        .set_shared(
            NSQConfigShared::new()
                .set_credentials(b"secret".to_vec())
                .set_tls(NSQConfigSharedTLS::new("test.com")),
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4151".to_string(),
        ]))
        .set_shared(
            NSQConfigShared::new()
                .set_credentials(b"secret".to_vec())
                .set_tls(NSQConfigSharedTLS::new("test.com")),
        )
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_inflight_10() {
    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4150").build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(10)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4150".to_string(),
        ]))
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn lookup_consume_basic() {
    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4150").build();

    let mut addresses = HashSet::new();
    addresses.insert("localhost:4161".to_string());

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Lookup(
            NSQConsumerLookupConfig::new()
                .set_addresses(addresses)
                .set_poll_interval(std::time::Duration::from_millis(10)),
        ))
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_deflate() {
    init();

    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4150")
        .set_shared(NSQConfigShared::new().set_compression(
            NSQConfigSharedCompression::Deflate(
                NSQDeflateLevel::new(3).unwrap(),
            ),
        ))
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4150".to_string(),
        ]))
        .set_shared(NSQConfigShared::new().set_compression(
            NSQConfigSharedCompression::Deflate(
                NSQDeflateLevel::new(3).unwrap(),
            ),
        ))
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_encryption() {
    init();

    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4151")
        .set_shared(
            NSQConfigShared::new()
                .set_tls(NSQConfigSharedTLS::new("test.com"))
                .set_credentials(b"secret".to_vec()),
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4151".to_string(),
        ]))
        .set_shared(
            NSQConfigShared::new()
                .set_tls(NSQConfigSharedTLS::new("test.com"))
                .set_credentials(b"secret".to_vec()),
        )
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_encryption_and_deflate() {
    init();

    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4151")
        .set_shared(
            NSQConfigShared::new()
                .set_tls(NSQConfigSharedTLS::new("test.com"))
                .set_compression(NSQConfigSharedCompression::Deflate(
                    NSQDeflateLevel::new(3).unwrap(),
                ))
                .set_credentials(b"secret".to_vec()),
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4151".to_string(),
        ]))
        .set_shared(
            NSQConfigShared::new()
                .set_tls(NSQConfigSharedTLS::new("test.com"))
                .set_compression(NSQConfigSharedCompression::Deflate(
                    NSQDeflateLevel::new(3).unwrap(),
                ))
                .set_credentials(b"secret".to_vec()),
        )
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_snappy() {
    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4150")
        .set_shared(
            NSQConfigShared::new()
                .set_compression(NSQConfigSharedCompression::Snappy),
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4150".to_string(),
        ]))
        .set_shared(
            NSQConfigShared::new()
                .set_compression(NSQConfigSharedCompression::Snappy),
        )
        .build();

    run_message_tests(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_snappy_large() {
    init();

    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let mut producer = NSQProducerConfig::new("localhost:4150")
        .set_shared(
            NSQConfigShared::new()
                .set_compression(NSQConfigSharedCompression::Snappy),
        )
        .build();

    let mut consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4150".to_string(),
        ]))
        .set_shared(
            NSQConfigShared::new()
                .set_compression(NSQConfigSharedCompression::Snappy),
        )
        .build();

    let mut large = Vec::new();
    large.resize(1024 * 1024, 0);

    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Healthy());
    producer.publish(&topic, large.clone()).await.unwrap();
    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, large);
    message.finish().await;
}

#[tokio::test]
async fn direct_connection_encryption_and_snappy() {
    init();

    let topic = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("localhost:4151")
        .set_shared(
            NSQConfigShared::new()
                .set_tls(NSQConfigSharedTLS::new("test.com"))
                .set_compression(NSQConfigSharedCompression::Snappy)
                .set_credentials(b"secret".to_vec()),
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec![
            "localhost:4151".to_string(),
        ]))
        .set_shared(
            NSQConfigShared::new()
                .set_tls(NSQConfigSharedTLS::new("test.com"))
                .set_compression(NSQConfigSharedCompression::Snappy)
                .set_credentials(b"secret".to_vec()),
        )
        .build();

    run_message_tests(topic, producer, consumer).await;
}
