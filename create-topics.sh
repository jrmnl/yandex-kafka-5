echo 'security.protocol=SASL_SSL' > /tmp/adminclient-configs.conf
echo 'sasl.mechanism=PLAIN' >> /tmp/adminclient-configs.conf
echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";' >> /tmp/adminclient-configs.conf
echo 'ssl.keystore.location=/etc/kafka/secrets/kafka.kafka-1.keystore.pkcs12' >> /tmp/adminclient-configs.conf
echo 'ssl.keystore.password=BrRsHy48f8bG7ULA' >> /tmp/adminclient-configs.conf
echo 'ssl.key.password=BrRsHy48f8bG7ULA' >> /tmp/adminclient-configs.conf
echo 'ssl.truststore.location=/etc/kafka/secrets/kafka.kafka-1.truststore.jks' >> /tmp/adminclient-configs.conf
echo 'ssl.truststore.password=BrRsHy48f8bG7ULA' >> /tmp/adminclient-configs.conf
echo 'ssl.endpoint.identification.algorithm=' >> /tmp/adminclient-configs.conf 

cub kafka-ready -b kafka-1:19093,kafka-2:19093,kafka-3:19093 1 120 --config /tmp/adminclient-configs.conf

kafka-topics \
  --bootstrap-server kafka-1:19093,kafka-2:19093,kafka-3:19093 \
  --command-config /tmp/adminclient-configs.conf \
  --create \
  --if-not-exists \
  --topic topic1 \
  --partitions 3 \
  --replication-factor 2 \
  --config min.insync.replicas=2

kafka-topics \
  --bootstrap-server kafka-1:19093,kafka-2:19093,kafka-3:19093 \
  --command-config /tmp/adminclient-configs.conf \
  --create \
  --if-not-exists \
  --topic topic2 \
  --partitions 3 \
  --replication-factor 2 \
  --config min.insync.replicas=2

kafka-acls \
  --bootstrap-server kafka-1:19093,kafka-2:19093,kafka-3:19093 \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:producer \
  --operation write \
  --topic topic1 \
  --topic topic2

kafka-acls \
  --bootstrap-server kafka-1:19093,kafka-2:19093,kafka-3:19093 \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:consumer \
  --operation read \
  --group app_group \
  --topic topic1

kafka-acls \
  --bootstrap-server kafka-1:19093,kafka-2:19093,kafka-3:19093 \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:producer \
  --operation write \
  --topic topic1 \
  --topic topic2
