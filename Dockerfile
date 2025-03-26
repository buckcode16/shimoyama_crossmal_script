# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements and install dependencies first (for caching)
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files (including your scripts and wrapper script)
COPY . .

# Ensure the wrapper script is executable
RUN chmod +x crossmall_daily.sh

# Use the wrapper script as the container's default command
CMD ["./crossmall_daily.sh"]
