import pandas as pd
import os
import re


class GrandLineProcessor:
    def __init__(self):
        self.root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_file = os.path.join(self.root_dir, "data", "raw_mal_reviews.csv")
        self.output_file = os.path.join(self.root_dir, "data", "labeled_one_piece_reviews.csv")

        # Dizionario Saghe (Bilanciato e focalizzato)
        self.saga_keywords = {
            'East Blue': ['east blue', 'romance dawn', 'arlong', 'buggy', 'loguetown', 'baratie', 'kuro', 'don krieg',
                          'shanks', 'morgan', 'alvida', 'zoro', 'nami', 'usopp', 'syrup village', 'orange town',
                          'gold roger', 'gum gum', 'mihawk', 'smoker', 'don krieg', 'zoro vs mihawk'],
            'Alabasta': ['alabasta', 'crocodile', 'vivi', 'chopper', 'drum island', 'baroque works', 'mr 0', 'mr 1',
                         'mr 2', 'poneglyph', 'nefertari', 'rainbase', 'whiskey peak', 'little garden', 'karoo', 'dory',
                         'brogy'],
            'Skypiea': ['enel', 'eneru', 'sky island', 'gan fall', 'upper yard', 'shandora', 'wyper', 'bellamy', 'jaya',
                        'golden bell', 'skypiea', 'skypia', 'conis', 'white sea', 'knock up stream'],
            'Water 7': ['lucci', 'cp9', 'robin', 'franky', 'enies lobby', 'going merry', 'thousand sunny', 'spandam',
                        'kaku', 'gear 2', 'gear 3', 'sogeking', 'iceburg', 'water 7', 'blueno', 'kalifa',
                        'crying over a ship'],
            'Thriller Bark': ['moria', 'brook', 'perona', 'nightmare luffy', 'ryuma', 'absalom', 'hogback', 'oars',
                              'laboon', 'thriller bark', 'kuma', 'shadows'],
            'Summit War': ['marineford', 'whitebeard', 'impel down', 'akainu', 'sengoku', 'garp', 'blackbeard', 'teach',
                           'magellan', 'hancock', 'rayleigh', 'kizaru', 'war of the best', 'shiryu', 'ivankov',
                           'time skip', 'timeskip', 'sabaody', '3d2y'],
            'Fish-Man Island': ['hody', 'shirahoshi', 'jimbei', 'jinbe', 'noah', 'sun pirates', 'vander decken',
                                'fisher tiger', 'poseidon', 'fishman island', 'neptune', 'joy boy'],
            'Dressrosa': ['doflamingo', 'law', 'fujitora', 'gear 4', 'colosseum', 'rebecca', 'kyros', 'sugar',
                          'corazon', 'sabo', 'dressrosa', 'trebol', 'pica', 'diamante', 'grand fleet', 'donquixote', 'punk hazard', 'brownbeard', 'heart pirates'],
            'Whole Cake': ['big mom', 'katakuri', 'pudding', 'germa', 'vinsmoke', 'bege', 'wedding cake', 'cracker',
                           'smoothie', 'whole cake', 'linlin', 'sanji backstory', 'wci'],
            'Wano-Egghead': ['kaido', 'kaidou', 'gear 5', 'nika', 'vegapunk', 'kuma', 'saturn', 'orochi', 'oden',
                             'akazaya', 'yamato', 'luffy gear 5', 'wano kuni', 'egghead', 'bonney', 'lucci awakened',
                             'stussy', 'kizaru egghead', 'onigashima', 'momonosuke', 'hiyori', 'york']
        }

        # Trigger Spoiler Raffinati (Nome + Azione o Termini Tecnici Univoci)
        self.spoiler_triggers = [
            # Eventi specifici (Nome + Azione)
            'ace dies', 'ace death', 'ace died', 'whitebeard death', 'whitebeard died',
            'merry death', 'merry died', 'merry burning', 'burning the merry',
            'sabo is alive', 'sabo returns', 'luffy gear 5', 'nika reveal',
            # Termini strutturali pesanti
            'timeskip', 'time skip', '2 years later', 'reunion at sabaody', '3d2y',
            # Termini tecnici di "svolta"
            'awakening', 'gear 5', 'gear 4', 'joy boy', 'imu', 'major spoiler',
            'heavy spoiler', 'plot twist', 'true identity'
        ]

    def sanitize_data(self, df):
        df['review_text'] = df['review_text'].fillna("").astype(str)
        # Filtro junk e bot
        df = df[~df['review_text'].str.strip().str.lower().isin(['more pics', 'more pics...', ''])]
        df['review_text'] = df['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()
        df = df.drop_duplicates(subset=['review_text'])
        return df

    def label_data(self):
        if not os.path.exists(self.input_file): return
        df = pd.read_csv(self.input_file)
        df = self.sanitize_data(df)

        # Pulizia per il mapping
        df['clean_text'] = df['review_text'].str.lower().str.replace(r'[^a-z0-9\s]', '', regex=True)
        df['saga'] = 'General/Unknown'
        df['is_spoiler'] = 0

        sagas_ordered = list(self.saga_keywords.keys())[::-1]

        for index, row in df.iterrows():
            text = row['clean_text']
            # 1. Mapping Saga
            for saga in sagas_ordered:
                if any(kw in text for kw in self.saga_keywords[saga]):
                    df.at[index, 'saga'] = saga
                    break

            # 2. Check Spoiler (con Word Boundary \b)
            is_spoil = False
            for trigger in self.spoiler_triggers:
                if re.search(rf'\b{re.escape(trigger)}\b', text):
                    is_spoil = True
                    break

            # Aggiungiamo il controllo generico se l'utente scrive esplicitamente "SPOILER"
            if is_spoil or "spoiler" in text:
                df.at[index, 'is_spoiler'] = 1

        df.to_csv(self.output_file, index=False)
        print(f"📊 Distribuzione Saghe:\n{df['saga'].value_counts()}")
        perc = (df['is_spoiler'].sum() / len(df) * 100) if len(df) > 0 else 0
        print(f"🕵️ Spoiler Certificati: {df['is_spoiler'].sum()} ({perc:.1f}%)")


if __name__ == "__main__":
    GrandLineProcessor().label_data()