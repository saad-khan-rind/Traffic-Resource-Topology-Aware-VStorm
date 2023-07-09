# Traffic-Resource-Topology-Aware-VStorm
# Running the Scheduler Independent of the UI with Apache Storm

This repository provides instructions for running the scheduler independent of the UI using Apache Storm. The steps outlined below will guide you through the setup and configuration process.

## Prerequisites

Before proceeding, ensure that you have the following prerequisites installed on your system:

- Apache Storm setup
- Maven (for compiling the project)
- ZooKeeper

## Installation and Configuration Steps

Follow these steps to set up and configure the scheduler:

1. Install the Apache Storm setup on your system. Refer to the Apache Storm documentation for detailed instructions.

2. Compile the Maven project that contains the scheduler code. This will generate a JAR file in the `target` folder of your Maven project.

3. Locate the JAR file that was generated and place it in the `lib` folder of your Apache Storm installation.

4. Rename the JAR file to "vStorm".

5. Open the `storm.yaml` file located in your Apache Storm installation directory.

6. Add the following line to the `storm.yaml` file:
   ```
   storm.scheduler: "org.apache.storm.scheduler.VStorm"
   ```
   This configuration tells Apache Storm to use the vStorm scheduler.

## Running the Scheduler and Apache Storm Components

To run the scheduler and Apache Storm components, follow these steps:

1. Open three separate terminals or command prompt windows.

2. Terminal 1:
   Start the ZooKeeper server by executing the command:
   ```
   zkServer.sh start
   ```
   If you are using Windows, the command might be different. Adjust accordingly.

   Start the Nimbus server by running the command:
   ```
   storm nimbus
   ```

3. Terminal 2:
   Start the Storm Supervisor by executing the command:
   ```
   storm supervisor
   ```

4. Terminal 3:
   Start the Storm UI by running the command:
   ```
   storm ui
   ```

By following these steps and running the respective commands on separate terminals, you will be able to run the scheduler independent of the UI with Apache Storm.

Please note that the commands provided may vary based on your operating system and specific Apache Storm setup. Refer to the Apache Storm documentation for further details and adjustments.

## Contributing

If you would like to contribute to this project, feel free to submit pull requests with improvements or fixes.

Happy scheduling with Apache Storm!
