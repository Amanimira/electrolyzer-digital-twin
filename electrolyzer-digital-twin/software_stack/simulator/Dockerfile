# Use an appropriate base image (e.g., Python slim for a simulator)
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /simulator

# Copy requirements file and install simulator dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulator source code
COPY . .

# Expose port for simulator if needed (example: 5000)
EXPOSE 5000

# Command to run the simulator
CMD ["python", "simulator.py"]