import rclpy
from rclpy.node import Node
from sensor_msgs.msg import JointState
import paho.mqtt.client as mqtt
import json
import os
import threading

class JointStatePublisher(Node):

    def __init__(self):
        super().__init__('joint_state_publisher')
        self.get_logger().info('Initializing ROS 2 Node...')
        
        # Load configuration
        self.load_configuration()

        # Initialize ROS 2 subscriptions based on config
        self.subscriptions = []
        for ros2_topic, mqtt_topic in self.ros2_to_mqtt.items():
            self.create_ros2_subscription(ros2_topic, mqtt_topic)
        
        # Initialize MQTT
        self.mqtt_client = mqtt.Client("ros2_mqtt_publisher")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_message = self.on_message

        self.get_logger().info(f'Connecting to MQTT broker at {self.broker_address}...')
        self.mqtt_client.connect(self.broker_address, 1883, 60)
        
        # Subscribe to MQTT topics
        for mqtt_topic, ros2_topic in self.mqtt_to_ros2.items():
            self.mqtt_client.subscribe(mqtt_topic)

        # Start MQTT loop in a separate thread
        self.mqtt_thread = threading.Thread(target=self.mqtt_client.loop_forever)
        self.mqtt_thread.start()

    def load_configuration(self):
        self.broker_address = None
        self.ros2_to_mqtt = {}
        self.mqtt_to_ros2 = {}

        config_file = os.path.join(os.path.dirname(__file__), 'config.txt')
        with open(config_file, 'r') as file:
            for line in file:
                if line.startswith('broker:'):
                    _, address = line.split(':')
                    self.broker_address = address.strip()
                elif line.startswith('ros2_to_mqtt:'):
                    _, mapping = line.split(':')
                    ros2_topic, mqtt_topic = mapping.strip().split()
                    self.ros2_to_mqtt[ros2_topic] = mqtt_topic
                elif line.startswith('mqtt_to_ros2:'):
                    _, mapping = line.split(':')
                    mqtt_topic, ros2_topic = mapping.strip().split()
                    self.mqtt_to_ros2[mqtt_topic] = ros2_topic

        if not self.broker_address:
            raise ValueError('MQTT broker address not specified in config file')

    def create_ros2_subscription(self, ros2_topic, mqtt_topic):
        subscription = self.create_subscription(
            JointState,
            ros2_topic,
            lambda msg, mt=mqtt_topic: self.listener_callback(msg, mt),
            10
        )
        self.subscriptions.append(subscription)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.get_logger().info('Connected to MQTT broker')
        else:
            self.get_logger().error(f'Failed to connect to MQTT broker, return code {rc}')

    def on_disconnect(self, client, userdata, rc):
        self.get_logger().info('Disconnected from MQTT broker')

    def on_message(self, client, userdata, msg):
        mqtt_topic = msg.topic
        ros2_topic = self.mqtt_to_ros2.get(mqtt_topic)
        if ros2_topic:
            self.get_logger().info(f'Received MQTT message on {mqtt_topic}, publishing to ROS 2 topic {ros2_topic}')
            # Convert the MQTT message back to a ROS 2 message
            msg_dict = json.loads(msg.payload)
            joint_state_msg = JointState()
            joint_state_msg.header.stamp.sec = msg_dict['header']['stamp']['sec']
            joint_state_msg.header.stamp.nanosec = msg_dict['header']['stamp']['nanosec']
            joint_state_msg.header.frame_id = msg_dict['header']['frame_id']
            joint_state_msg.name = msg_dict['name']
            joint_state_msg.position = msg_dict['position']
            joint_state_msg.velocity = msg_dict['velocity']
            joint_state_msg.effort = msg_dict['effort']
            self.publish_ros2_message(ros2_topic, joint_state_msg)

    def publish_ros2_message(self, topic, msg):
        # Create a publisher for the topic if it doesn't exist
        if not hasattr(self, 'publishers'):
            self.publishers = {}
        if topic not in self.publishers:
            self.publishers[topic] = self.create_publisher(JointState, topic, 10)
        publisher = self.publishers[topic]
        publisher.publish(msg)

    def listener_callback(self, msg, mqtt_topic):
        # Convert the ROS message to a dictionary
        msg_dict = {
            'header': {
                'stamp': {
                    'sec': msg.header.stamp.sec,
                    'nanosec': msg.header.stamp.nanosec
                },
                'frame_id': msg.header.frame_id
            },
            'name': msg.name,
            'position': msg.position,
            'velocity': msg.velocity,
            'effort': msg.effort
        }
        
        # Convert the dictionary to a JSON string
        msg_json = json.dumps(msg_dict)
        
        # Publish to MQTT
        self.mqtt_client.publish(mqtt_topic, msg_json)
        self.get_logger().info(f'Received message and published to MQTT: {msg_json}')

def main(args=None):
    rclpy.init(args=args)
    joint_state_publisher = JointStatePublisher()
    joint_state_publisher.get_logger().info('ROS 2 Node initialized and spinning...')
    rclpy.spin(joint_state_publisher)

    # Shutdown
    joint_state_publisher.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()
