from setuptools import find_packages, setup

package_name = 'ros2mqtt'

setup(
    name=package_name,
    version='0.0.1',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='goerk',
    maintainer_email='goerkem.can.ertemli@rwth-aachen.de',
    description='Simple ROS2 to MQTT message bridge',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'ros2mqtt = ros2mqtt.ros2mqtt:main',
        ],
    },
)

