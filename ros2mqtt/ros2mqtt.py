import rclpy
from rclpy.node import Node
from sensor_msgs.msg import JointState
import paho.mqtt.client as mqtt
import json

global broker_add
broker_add = "mqtt-dashboard.com"

class JointStatePublisher(Node):

    def __init__(self):
        super().__init__('joint_state_publisher')
        self.get_logger().info('Initializing ROS 2 Node...')
        
        # ROS 2 subscription
        self.subscription = self.create_subscription(
            JointState,
            'joint_states',
            self.listener_callback,
            10)
        self.subscription  # prevent unused variable warning
        
        # MQTT setup
        self.mqtt_client = mqtt.Client("ros2_mqtt_publisher")
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        
        self.get_logger().info('Connecting to MQTT broker...')
        self.mqtt_client.connect(broker_add, 1883, 60)
        self.mqtt_client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.get_logger().info('Connected to MQTT broker')
        else:
            self.get_logger().error(f'Failed to connect to MQTT broker, return code {rc}')

    def on_disconnect(self, client, userdata, rc):
        self.get_logger().info('Disconnected from MQTT broker')

    def listener_callback(self, msg):
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
        self.mqtt_client.publish("jekko/jointstate", msg_json)
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
