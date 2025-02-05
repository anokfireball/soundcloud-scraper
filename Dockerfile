FROM python:3.11-alpine

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

ENV ONESHOT=true
ENV DATA_DIR=data
ENV SOUNDCLOUD_USERNAME=soundcloud
ENV API_CLIENT_ID=YOUR_CLIENT_ID
ENV API_CLIENT_SECRET=YOUR_CLIENT_SECRET
ENV API_REDIRECT_URI=http://localhost:8080/callback
ENV WEBHOOK=http://localhost:8080

CMD ["python", "src/main.py"]
