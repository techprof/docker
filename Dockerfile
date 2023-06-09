#the repository URL https://github.com/techprof/docker is used. Replace it with the actual URL of the Git repository you want to clone.

#The sample dbt commands dbt clean, dbt compile, and dbt run are included in the script. You can modify these commands based on your specific dbt workflow.

#Make sure to have a requirements.txt file containing the necessary Python libraries for your project in the same directory as the Dockerfile.

#Remember to build the Docker image using the docker build command and then run a container based on the image using the docker run command.



# Use the official Ubuntu base image
FROM ubuntu:latest

# Update the package lists
RUN apt-get update

# Install dependencies: curl, git
RUN apt-get install -y curl git

# Install Visual Studio Code
RUN curl -fsSL https://code.visualstudio.com/docs/?dv=linux64_deb | apt-key add -
RUN echo "deb [arch=amd64] https://packages.microsoft.com/repos/vscode stable main" | tee /etc/apt/sources.list.d/vscode.list
RUN apt-get update
RUN apt-get install -y code

# Install Python and pip
RUN apt-get install -y python3 python3-pip

# Install virtualenv
RUN pip3 install virtualenv

# Create and activate the virtual environment
RUN virtualenv venv
RUN . venv/bin/activate

# Install Python libraries
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Clone the Git repository
RUN git clone https://github.com/techprof/docker app

# Change working directory
WORKDIR /app

# Run dbt specific commands
RUN dbt clean
RUN dbt compile
RUN dbt run

# Set the default command
CMD ["bash"]

