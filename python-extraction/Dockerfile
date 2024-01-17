# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory in the container
WORKDIR /app

# Copy the the python udp server sript and the requirements in the working dir
COPY keepalive.py .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Run udp_server.py when the container launches
CMD ["python", "keepalive.py"]
