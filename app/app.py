import torch
from flask import Flask, request, jsonify
from transformers import RobertaTokenizer

app = Flask(__name__)

# Initialize a tokenizer using the 'roberta-base' pre-trained model
tokenizer = RobertaTokenizer.from_pretrained('roberta-base')
model = torch.load('trained_model.pth')


def analyze_sentiment(input_str):
    model.eval()  # Set to evaluation mode if you're doing inference

    # Tokenizing the input text
    inputs = tokenizer(input_str, return_tensors="pt")
    outputs = model(**inputs)

    # Apply softmax to convert logits to probabilities
    probabilities = torch.softmax(outputs.logits, dim=-1)

    # Get the predicted label index
    predicted_label_index = torch.argmax(probabilities, dim=-1).item()

    # Optional: If you have a dictionary of id to label, you can print the label
    labels_dict = {0: "negative", 1: "neutral", 2: "positive"}
    predicted_label = labels_dict[predicted_label_index]

    return predicted_label


@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    return 'Success', 200


@app.route('/predict', methods=['POST'])
def predict():
    # Check if request is in JSON format and contains 'sentence'
    if not request.json or 'sentence' not in request.json:
        return jsonify({'error': 'Missing sentence in JSON data'}), 400

    try:
        prediction = analyze_sentiment(request.json['sentence'])
        return jsonify({'prediction': prediction}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
