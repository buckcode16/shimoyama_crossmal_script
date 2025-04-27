# Use an official lightweight Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements and install dependencies first (for caching)
COPY requirements.txt .

# Consider adding --no-cache-dir to pip install to reduce image size
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files (including your scripts and wrapper script)
COPY . .

# Ensure the NEW wrapper scripts are executable
RUN chmod +x run_all_tasks.sh run_stock_only.sh
