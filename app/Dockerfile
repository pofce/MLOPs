FROM python:3.11-slim-buster

# Setting the working directory in the container
WORKDIR /app

# Copy only the requirements.txt initially to leverage Docker cache
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY app.py ./
COPY trained_model.pth ./

# Expose port 5000 to the host
EXPOSE 5000

# Command to run the application
CMD ["flask", "run", "--host=0.0.0.0"]

# docker build -t model_api .
# docker run --name serving_model -p 5000:5000 -d model_api