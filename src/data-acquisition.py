import kagglehub
import pandas as pd
import os


def final_extraction():
    # 1. Configurazione Percorsi
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, "data")
    output_file = os.path.join(data_dir, "raw_mal_reviews.csv")

    # Puntiamo alla cache dove kagglehub ha scaricato i dati
    dataset_slug = "marlesson/myanimelist-dataset-animes-profiles-reviews"
    download_path = kagglehub.dataset_download(dataset_slug)
    csv_path = os.path.join(download_path, "reviews.csv")

    print(f"🏴‍☠️ Estrazione in corso da: {csv_path}")

    # 2. Caricamento e Filtraggio
    # Usiamo 'anime_uid' (ID 21) e 'text' come confermato dalla diagnosi
    df = pd.read_csv(csv_path)

    # Filtro One Piece
    op_df = df[df['anime_uid'] == 21].copy()

    # Pulizia Base e Ridenominazione
    op_df = op_df[['text', 'score']].rename(columns={'text': 'review_text'})
    # Rimuoviamo eventuali righe con testo vuoto
    op_df = op_df.dropna(subset=['review_text'])

    # 3. Salvataggio Atomico
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    op_df.to_csv(output_file, index=False, encoding='utf-8')

    print(f"\n🏆 OPERAZIONE COMPLETATA CON SUCCESSO!")
    print(f"📊 Recensioni One Piece estratte: {len(op_df)}")
    print(f"📍 File pronto per il Machine Learning: {output_file}")
    print("\n⚠️ PROSSIMO PROBLEMA: Il dataset non ha etichette 'is_spoiler'.")


if __name__ == "__main__":
    final_extraction()