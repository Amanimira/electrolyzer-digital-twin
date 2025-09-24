# Use an official Node.js runtime as the base image (adjust based on your backend tech)
FROM node:18-alpine

# Set working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json to install dependencies
COPY package*.json ./

# Install backend dependencies
RUN npm install --production

# Copy the backend source code
COPY . .

# Expose backend port (example: 3000)
EXPOSE 3000

# Command to run the backend service
CMD ["node", "server.js"]