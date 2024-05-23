# DistributedFileSystem

## Description
This project is a Distributed File System that enables low storage devices to train image data on the device itself, without requiring large amounts of local storage. The system utilizes Apache Kafka to distribute batches of images to worker nodes, which then send the images to a head node for model training.

## Installation
- Clone the repository
- Download and setup kafka locally 
- Download cifar-10 Dataset 


## Technologies Used
- Apache Kafka
- Java
- Machine Learning
- Image Processing
- Distributed File Systems

## Architecture
The system consists of three consumer nodes with one kafka broker and one head node. The consumer nodes read images from the local device and send them to the Kafka broker based on the read write speeds in the consumer nodes, which distributes the images to the head node for training.
