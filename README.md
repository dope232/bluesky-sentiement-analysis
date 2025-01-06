# Bluesky Sentiment Analysis Pipeline

A real-time sentiment analysis system that processes Bluesky posts using Apache Kafka, Spark, and SQLite. The system analyzes posts for sentiment and visualizes the results using Streamlit.

## Architecture

```
Bluesky API → Kafka → Spark → SQLite → Streamlit
```


- Python 3.8+
- Apache Kafka
- Apache Spark
- SQLite
- Streamlit

## TODO List


- [ ] Implement error handling for API connection drops
- [ ] Add logging throughout the pipeline
- [ ] Create configuration file for easy setup
- [ ] Upgrade to a multi-class emotion model
  - Consider models like "j-hartmann/emotion-english-distilroberta-base"
  - Add support for emotions like joy, sadness, anger, fear, surprise
- [ ] Add support for multiple languages

- [ ] Add real-time updating dashboards
- [ ] Implement websocket connections for live updates


