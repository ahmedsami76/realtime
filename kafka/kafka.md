
## **Part 1: Introduction to Apache Kafka**

*(Visual Cue: Show the Apache Kafka logo alongside a high-level architecture diagram. Place "Producers" on the left, a central "Kafka Cluster" in the middle, and "Consumers" on the right.)*

Before we write any code, we need to understand what Kafka is and why it exists. Apache Kafka is a distributed event streaming platform used to read, write, store, and process streams of events in real-time.

**Core Concepts to Explain on Camera:**

* **Events/Messages:** A record of something that happened (e.g., a user clicked a button, a sensor reported a temperature).
* **Topics:** The core organizational unit in Kafka. Think of it like a folder on your computer where specific types of events are stored.
* **Partitions:** Topics are broken down into partitions to allow Kafka to scale horizontally across multiple servers.
* **Producers:** Applications that write data to topics.
* **Consumers:** Applications that read data from topics.
* **KRaft Mode:** Note that as of recent versions, Kafka has removed its dependency on ZooKeeper for cluster metadata management. We will be using the modern, standalone KRaft mode.

---

## **Part 2: Hands-on Docker Installation**

*(Visual Cue: Screen record opening Visual Studio Code to an empty directory. Show the terminal at the bottom of the screen.)*

We are going to run Kafka using its official Docker image (`apache/kafka:4.3.0`). This is the cleanest way to set up a local development environment.

**Step 1: Start the Broker**
Run the following command in your VS Code terminal to download and start a single-node Kafka cluster in the background:

```bash
docker run -d --name kafka-broker -p 9092:9092 apache/kafka:4.3.0

```

**Step 2: Verify the Container**
Check that the container is running smoothly:

```bash
docker ps

```

---

## **Part 3: Basic Kafka Tasks (CLI)**

*(Visual Cue: Split screen showing your terminal on the left and a conceptual diagram of a Topic being created on the right.)*

Now that the broker is running, let's interact with it. Because we are using the official Docker image, the Kafka binary scripts are safely tucked away in the `/opt/kafka/bin/` absolute path inside the container. We will use `docker exec` to run these scripts directly from our host machine.

**Step 1: Create a Topic**
We need a place to send our data. Let's create a topic called `test-events`:

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic test-events --bootstrap-server localhost:9092

```

**Step 2: Produce a Message**
Let's open an interactive producer shell to write manual messages.

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-producer.sh --topic test-events --bootstrap-server localhost:9092

```

(Instructor Note: Type a few messages like "Hello World" and "Kafka is awesome" and hit Enter after each. Then use Ctrl+C to exit.)

**Step 3: Consume the Messages**
Let's read those messages back from the beginning of the topic.

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --topic test-events --from-beginning --bootstrap-server localhost:9092

```

---

## **Part 4: Visualizing the Cluster (Web UI)**

*(Visual Cue: Show a side-by-side comparison of the terminal window and the sleek, graphical dashboard of the Kafka UI. Highlight the "Topics" tab on the UI.)*

While the CLI is powerful, sometimes you just need to see your data. We are going to deploy UI for Apache Kafka, a free, open-source web interface that lets us monitor our cluster, view messages, and manage topics without typing long commands. It is lightweight, beautiful, and runs as a Docker container, which perfectly complements the architecture we have already built.

**Step 1: Spin up the UI Container**
Since we are already using Docker, we can launch the UI and connect it to our running Kafka broker with a single command:

```bash
docker run -d --name kafka-ui \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME="Local-Tutorial-Cluster" \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS="host.docker.internal:9092" \
  provectuslabs/kafka-ui:latest

```

(Instructor Note: Remind viewers on Linux that they may need to use their machine's local IP address or `--network host` instead of `host.docker.internal` to connect the containers!).

**Step 2: Explore the Dashboard**

1. Open your web browser and navigate to `http://localhost:8080`.

2. You will immediately see your "Local-Tutorial-Cluster" dashboard showing the health of the broker.


**Step 3: UI Walkthrough (Live Demo)**
*(Visual Cue: Record your screen navigating the UI in real-time).*
* **Topics Tab:** Click here to see the `test-events` topic we created earlier.
* **Messages:** Dive into the topic and look at the actual "Hello World" messages we produced in the previous step. Show how you can filter by offset or timestamp.
* **Consumers Tab:** Briefly show this empty tab, teasing that it will populate when we build our Python consumer in the next section.

---

## **Part 5: Leveling Up with Docker Compose**

*(Visual Cue: Show a diagram of two interlocking puzzle pieces, one labeled "Kafka Broker" and the other "Kafka UI," coming together into a single "Docker Network" box.)*

Running individual Docker commands is great for learning the mechanics, but in a real-world Data Engineering workflow, we want our infrastructure to be reproducible. Instead of memorizing long commands, we are going to define our entire environment—both the Kafka broker and the visual UI—in a single file.

This also solves a common networking issue: by putting both containers in the same Docker Compose file, they automatically share a network and can talk to each other using their service names!.

**Step 1: Create the Manifest**
In the root directory of your VS Code project, create a file named `docker-compose.yml` and add the following configuration:

```yaml
version: '3.8'

services:
  kafka-broker:
    image: apache/kafka:4.3.0
    container_name: kafka-broker
    ports:
      - "9092:9092"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: "Local-Tutorial-Cluster"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka-broker:9092"
    depends_on:
      - kafka-broker

```

**Step 2: Spin It All Up**
Stop and remove your previous individual containers, then launch your entire stack with one elegant command:

```bash
docker-compose up -d

```

(Instructor Note: Remind viewers that the `-d` flag runs everything in detached mode, so you get your terminal prompt back immediately.)

**Step 3: Verify the Stack**
Run `docker ps` to see both containers running side-by-side. You can now interact with the Kafka broker via your terminal on port 9092, or open `http://localhost:8080` in your browser to see the Kafka UI.

---

## **Part 6: End-to-End Sample Project (Python & Public Data)**

*(Visual Cue: Show a data flow diagram: Wikimedia Recent Changes API -> Python Producer -> Kafka Docker Broker -> Python Consumer -> Aggregated Output.)*

For our real-world project, we will consume the live stream of edits happening across Wikipedia (Wikimedia Recent Changes SSE) and push them into Kafka. A separate consumer will read these edits and filter them.

**Step 1: Set up the Python Environment**
In your VS Code terminal, set up your virtual environment and install the required official open-source libraries:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install confluent-kafka requests sseclient-py

```

**Step 2: Create the Topic**

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic wikimedia-edits --bootstrap-server localhost:9092

```

**Step 3: The Python Producer (`producer.py`)**
Create this file in VS Code to fetch the live stream and produce it to Kafka.

```python
import json
from sseclient import SSEClient as EventSource
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    print("Connecting to Wikimedia Stream...")
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                # We only care about Wikipedia edits
                if change.get('server_name') == 'en.wikipedia.org':
                    user = change.get('user', 'unknown')
                    title = change.get('title', 'unknown')
                    
                    record = {"user": user, "title": title}
                    
                    producer.produce(
                        'wikimedia-edits', 
                        key=user.encode('utf-8'),
                        value=json.dumps(record).encode('utf-8'), 
                        callback=delivery_report
                    )
                    producer.poll(0)
            except ValueError:
                pass

if __name__ == '__main__':
    main()

```

**Step 4: The Python Consumer (`consumer.py`)**
Create this file to read the data in real-time.

```python
import json
from confluent_kafka import Consumer, KafkaError

def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'wiki-analytics-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['wikimedia-edits'])
    print("Listening for Wikipedia edits...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            data = json.loads(msg.value().decode('utf-8'))
            print(f"NEW EDIT: User '{data['user']}' edited page '{data['title']}'")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()

```

**Step 5: Run the Pipeline**

1. Open a terminal and run `python producer.py`.
2. Open a second terminal and run `python consumer.py`.
3. Watch the real-time data flow through your local Kafka cluster!.

---

## **Part 7: Scaling to Kubernetes with K3s**

*(Visual Cue: Show a diagram of a ship's steering wheel (Kubernetes logo) wrapping around the Kafka and Python logos. On screen, highlight the terms "Pods," "Services," and "Deployments.")*

Up until now, we have been running our Apache Kafka platform using Docker. However, in enterprise Big Data environments, workloads are orchestrated using Kubernetes. To simulate a production environment locally without melting our laptops, we are going to use K3s—a highly optimized, lightweight Kubernetes distribution perfect for edge computing and local testing.

**Core Concepts to Explain on Camera:**

* **Pods:** The smallest deployable units in Kubernetes. Our Kafka broker and Kafka UI will each live inside their own Pods.
* **Deployments:** The blueprint that tells Kubernetes how to keep our Pods running smoothly.
* **Services:** The networking layer that gives our Pods stable IP addresses so they can talk to each other (and to our local Python scripts).


**Step 1: Installing K3s**
Since we are focusing on purely open-source, industry-standard tools, installing K3s on a Linux machine is beautifully simple. Run the installation script:

```bash
curl -sfL https://get.k3s.io | sh -

```

**Step 2: Deploying Kafka to K3s**
Instead of imperative `docker run` commands, Kubernetes uses declarative YAML manifests. We will describe exactly what we want our Kafka architecture to look like, and K3s will build it.

Create `kafka-deployment.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-service
spec:
  selector:
    app: kafka
  ports:
    - name: external
      protocol: TCP
      port: 9092
      targetPort: 9092
    - name: internal
      protocol: TCP
      port: 29092
      targetPort: 29092
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-broker
spec:
  type: LoadBalancer
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: apache/kafka:4.3.0
        ports:
        - containerPort: 9092
        - containerPort: 29092
        env:
        - name: KAFKA_NODE_ID
          value: "1"
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_LISTENERS
          value: "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "EXTERNAL://localhost:9092,INTERNAL://kafka-service:29092"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
        - name: KAFKA_INTER_BROKER_LISTENER_NAME
          value: "INTERNAL"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: "1@localhost:9093"
```

Apply the manifest:

```bash
sudo k3s kubectl apply -f kafka-deployment.yaml

```

**Step 3: Port-Forwarding and Topic Creation**
Because Kubernetes isolates its network, we need to open a secure tunnel from our local host machine to the Kafka Service so our Python scripts can reach it. Run this and leave it open:

```bash
sudo k3s kubectl port-forward svc/kafka-service 9092:9092

```

Just like we used `docker exec` before, we now use `kubectl exec` to run the Kafka binary scripts tucked away in the `/opt/kafka/bin/` absolute path.

```bash
sudo k3s kubectl exec -it <your-pod-name> -- /opt/kafka/bin/kafka-topics.sh --create --topic wikimedia-edits --bootstrap-server localhost:9092

```

**Step 4: Visualizing the K3s Cluster**
Let's bring our visual dashboard back, this time deployed natively inside K3s alongside our broker. Create `kafka-ui-deployment.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui-service
spec:
  type: LoadBalancer
  selector:
    app: kafka-ui
  ports:
    - protocol: TCP
      port: 8080
      targetPort: 8080
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ui
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-ui
  template:
    metadata:
      labels:
        app: kafka-ui
    spec:
      containers:
      - name: kafka-ui
        image: provectuslabs/kafka-ui:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_CLUSTERS_0_NAME
          value: "K3s-Tutorial-Cluster"
        - name: KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS
          value: "kafka-service:29092"
```

Apply the manifest and port-forward the UI service to port 8080.

You are right at the finish line for getting your visual dashboard up and running in K3s! Once you have created and saved the `kafka-ui-deployment.yaml` file, here is exactly how you proceed based on your masterclass outline:

### **Next Steps to Launch the UI**

1. **Apply and Port-Forward:** First, you need to deploy the UI and open a tunnel to it. You will apply the manifest and port-forward the UI service to port 8080. *(Using the K3s commands from earlier in your script, you would run `sudo k3s kubectl apply -f kafka-ui-deployment.yaml` and then `sudo k3s kubectl port-forward svc/kafka-ui-service 8080:8080`)*.

2. **Verify the Deployment:** Once the tunnel is open, go ahead and open `http://localhost:8080` in your web browser.

3. **Explore:** You will see your K3s-hosted Kafka cluster ready to go!.

> 
> **Instructor Note for Python Pipeline:** Because we set up the `kubectl port-forward` for port 9092 in Part 4, your viewers do not need to change a single line of code in the `producer.py` or `consumer.py` scripts from the previous chapter. The Python `confluent_kafka` library will continue connecting to `localhost:9092`, oblivious to the fact that the backend infrastructure has been completely upgraded to a Kubernetes cluster!.
> 
>