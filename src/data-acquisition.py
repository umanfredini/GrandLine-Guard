import praw
import pandas as pd
import os
from dotenv import load_dotenv

# Caricamento credenziali dal file .env (Sicurezza)
load_dotenv()


def get_reddit_instance():
    """Inizializza l'istanza di Reddit con le tue future API."""
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent="python:GrandLineGuard:v1.0 (by /u/YourUsername)"
    )


def fetch_spoiler_data(limit_per_saga=100):
    """Estrae dati da Reddit basandosi su keyword e flair."""
    reddit = get_reddit_instance()
    subreddit = reddit.subreddit("OnePiece")

    # Dizionario delle saghe (il nostro obiettivo di classificazione)
    sagas = {
        "0_EastBlue": ["arlong", "don krieg", "kuro", "buggy", "loguetown"],
        "1_Alabasta": ["crocodile", "vivi", "baroque works", "nefertari"],
        "2_Skypiea": ["ener", "enel", "upper yard", "shandora", "wyper"],
        "3_Water7": ["franky", "galley-la", "iceburg", "cp9", "treno marino"],
        "4_EniesLobby": ["gear 2", "gear 3", "sogeking", "lucci", "spandam"],
        "5_Marineford": ["ace", "barbabianca", "whitebeard", "akainu", "marineford"],
        "6_PostMarineford": ["timeskip", "hody", "caesar", "punk hazard"],
        "7_Dressrosa": ["doflamingo", "gear 4", "fujitora", "corazon", "doflamingo"],
        "8_Imperatori": ["big mom", "kaido", "katakuri", "gear 5", "nika", "wano"],
        "9_SagaFinale": ["egghead", "vegapunk", "kizaru", "saturn", "astri"]
    }

    collected_data = []

    print("🏴‍☠️ Inizio raccolta dati dalla Rotta Maggiore...")

    # Strategia: cerchiamo nei post con tag spoiler per massimizzare i risultati
    for submission in subreddit.search("flair:'Manga Spoilers'", limit=200):
        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            text = comment.body.lower()

            # Labeling automatico basato su keyword (da raffinare nel preprocessing)
            for saga, keywords in sagas.items():
                if any(kw in text for kw in keywords):
                    collected_data.append({
                        "raw_text": comment.body,
                        "label": saga,
                        "source_post": submission.title
                    })
                    break  # Passa al commento successivo una volta trovata la saga

    return pd.DataFrame(collected_data)


if __name__ == "__main__":
    try:
        df = fetch_spoiler_data()
        # Salvataggio del dataset grezzo
        df.to_csv("data/raw_spoiler_data.csv", index=False)
        print(f"✅ Successo! Raccolti {len(df)} esempi.")
        print("\nDistribuzione iniziale delle Saghe (Analisi dati sbilanciati):")
        print(df['label'].value_counts())
    except Exception as e:
        print(f"❌ Errore (probabilmente mancano le API): {e}")