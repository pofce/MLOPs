FROM python:3.11-slim-buster

# Setting the working directory in the container
WORKDIR /app

# Copy only the requirements.txt initially to leverage Docker cache
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip install --upgrade "mlflow-skinny[databricks]>=2.5.0"

# Copy the rest of the application code
COPY app.py ./
COPY .databrickscfg /root/.databrickscfg

# Expose port 5000 to the host
EXPOSE 5050

# Command to run the application
CMD ["flask", "run", "--host=0.0.0.0", "--port=5050"]

# docker build -t model_api .
# docker run --name serving_model -p 5000:5000 -d model_api