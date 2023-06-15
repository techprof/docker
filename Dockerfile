#the repository URL https://github.com/techprof/docker is used. Replace it with the actual URL of the Git repository you want to clone.

#The sample dbt commands dbt clean, dbt compile, and dbt run are included in the script. You can modify these commands based on your specific dbt workflow.

#Make sure to have a requirements.txt file containing the necessary Python libraries for your project in the same directory as the Dockerfile.

#Remember to build the Docker image using the docker build command and then run a container based on the image using the docker run command.

# In this updated script, after switching to the non-root user, we append the command source /venv/bin/activate to the ~/.bashrc file to automatically activate the Python virtual environment whenever the user logs in.

# Use the Ubuntu base image
FROM ubuntu:latest

# Pull the Ubuntu image from Docker Hub
RUN docker pull ubuntu:latest

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    git \
    python3 \
    python3-pip

# Install Visual Studio Code
RUN curl -fsSL https://code.visualstudio.com/docs/?dv=linux64_deb > code.deb && \
    dpkg -i code.deb && \
    apt-get install -f -y && \
    rm code.deb

# Install Python packages
RUN pip3 install virtualenv

# Create and activate Python virtual environment
RUN virtualenv /venv
ENV PATH="/venv/bin:$PATH"

# Set user-specific PATH
ENV PATH="/home/user/bin:$PATH"

# Create a non-root user
RUN useradd -ms /bin/bash user
USER user
WORKDIR /home/user

# Activate Python virtual environment
RUN echo "source /venv/bin/activate" >> ~/.bashrc

# Install Python libraries
COPY requirements.txt .
RUN pip3 install --user -r requirements.txt

# Clone the Git repository
RUN git clone <git_repo_url>

# Set working directory
WORKDIR /home/user/path/to/repo

# Run specific commands using dbt (replace with your commands)
RUN dbt clean && \
    dbt debug && \
    dbt run

# Set the default command to run when the container starts
CMD ["bash", "--rcfile", "/home/user/.bashrc"]
