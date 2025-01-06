import streamlit as st 
import psycopg2
import time 
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import json



def get_data_from_psql():

    try:
        conn = psycopg2.connect(
            dbname="sentences",
            user="my_user",
            password="password",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM sentiment_analysis") # gives you sentence and sentiment. 
        data = cur.fetchall()
        cur.close()
        conn.close()
        return data


    except Exception as e:
        st.error(f"Error: {e}")



def get_sentiment(data):
    positive = 0
    negative = 0
    for sentence, sentiment in data:
        if sentiment == "positive":
            positive += 1
        else:
            negative += 1
    return positive, negative


def get_wordcloud(data, sentiment):
    text = " ".join([sentence for sentence, sent in data if sent == sentiment])
    wordcloud = WordCloud().generate(text)
    return wordcloud



def main():
    st.title("Sentiment Analysis")
    st.write("Sentiment analysis pie chart")
    data = get_data_from_psql()
    positive, negative = get_sentiment(data)
    st.write(f"Positive: {positive}")
    st.write(f"Negative: {negative}")
    st.write("Pie chart")
    fig, ax = plt.subplots()
    ax.pie([positive, negative], labels=["Positive", "Negative"], autopct="%1.1f%%")
    st.pyplot(fig)
    # ========================

    st.write("Word cloud of positive sentences")
    positive_wordcloud = get_wordcloud(data, "positive")
    st.write("Word cloud of negative sentences")
    negative_wordcloud = get_wordcloud(data, "negative")
    fig, ax = plt.subplots(1, 2)
    ax[0].imshow(positive_wordcloud)
    ax[0].axis("off")
    ax[0].set_title("Positive")
    ax[1].imshow(negative_wordcloud)
    ax[1].axis("off")
    ax[1].set_title("Negative")
    st.pyplot(fig)

  






if __name__ == "__main__":
    main()







