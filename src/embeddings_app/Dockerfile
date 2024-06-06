FROM nvidia/cuda:11.8.0-base-ubuntu22.04

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
        python3-pip \
        python3 \
        libglib2.0-0
# Install any python packages you need
COPY requirements.txt requirements.txt
RUN python3 -m pip install -r requirements.txt

# Upgrade pip
RUN python3 -m pip install --upgrade pip

# Install PyTorch and torchvision
RUN pip3 install torch --index-url https://download.pytorch.org/whl/cu118

# Set the working directory
WORKDIR /app

# Set the entrypoint
COPY . .
# Expose the port and run the application as the "root" user
EXPOSE 5000

#ENTRYPOINT [ "python3" ]

CMD ["sh", "run.sh"]