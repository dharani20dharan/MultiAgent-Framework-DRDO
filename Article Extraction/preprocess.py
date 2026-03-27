import json
import re
import warnings
from pathlib import Path
from typing import List, Optional

# Surpress dateparser warnings and huggingface warnings
warnings.filterwarnings("ignore")

import spacy
import dateparser

try:
    from fastcoref import FCoref
    HAS_FASTCOREF = True
except ImportError:
    HAS_FASTCOREF = False
    print("WARNING: fastcoref not installed. Coreference resolution will be skipped.")
    print("Install it with: pip install fastcoref")

class Preprocessor:
    def __init__(self):
        print("Loading NLP models...")
        # Load spaCy for sentence segmentation and tokenization
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("Downloading spacy en_core_web_sm model...")
            spacy.cli.download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
            
        # Optional: Load Coreference model
        self.coref_model = FCoref(device='cpu') if HAS_FASTCOREF else None
        print("Models loaded successfully.")

    def clean_text(self, text: str) -> str:
        """Step 1: Text Cleaning (Remove HTML, ads fluff, normalize space)."""
        if not text:
            return ""
        
        # Remove HTML tags (if any lingering after extractor)
        text = re.sub(r'<[^>]*>', ' ', text)
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        text = re.sub(r'www\.\S+', '', text)
        
        # Remove Wikipedia-specific artifacts like [edit], [1], or [citation needed]
        text = re.sub(r'\[(edit|citation needed|\d+)\]', '', text, flags=re.IGNORECASE)
        
        # Remove invisible/control characters but keep standard punctuation for sentences
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', text)
        
        # Collapse multiple spaces into one
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def resolve_coreferences(self, text: str) -> str:
        """Step 3: Pronoun resolution (e.g. 'He' -> 'Elon Musk')."""
        if not self.coref_model or not text.strip():
            return text
            
        try:
            preds = self.coref_model.predict(texts=[text])
            resolved_text = preds[0].get_resolved_text()
            return resolved_text
        except Exception as e:
            print(f"Coref error: {e}. Returning original text.")
            return text

    def normalize_dates(self, text: str) -> str:
        """Step 4: Normalization (Dates/Numbers) 
        Uses regex to find potential date phrases, passes to dateparser.
        This is a heuristic implementation; in production, token-level NER date normalization is safer.
        """
        # A simple heuristic: search for Month Day, Year formats
        # e.g., March 10, 2026
        date_pattern = re.compile(r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}', re.IGNORECASE)
        
        def replace_date(match):
            date_str = match.group(0)
            parsed_date = dateparser.parse(date_str)
            if parsed_date:
                return parsed_date.strftime("%Y-%m-%d")
            return date_str

        normalized = date_pattern.sub(replace_date, text)
        return normalized

    def segment_sentences(self, text: str) -> List[str]:
        """Step 2: Sentence Segmentation"""
        doc = self.nlp(text)
        return [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 3]

    def process_article(self, raw_text: str) -> List[str]:
        """Full pipeline processing for an article's text."""
        # 1. Cleaning
        cleaned = self.clean_text(raw_text)
        
        # 2. Pronoun resolution (Must be done on full text for context)
        resolved = self.resolve_coreferences(cleaned)
        
        # 3. Sentence Segmentation
        sentences = self.segment_sentences(resolved)
        
        # 4. Normalization
        normalized_sentences = [self.normalize_dates(sent) for sent in sentences]
        
        return normalized_sentences

def main():
    input_file = Path("test_results.jsonl")
    output_file = Path("preprocessed_sentences.jsonl")
    
    if not input_file.exists():
        print(f"Error: {input_file} not found. Run main.py first.")
        return

    processor = Preprocessor()
    print(f"Reading from {input_file}...")
    
    total_sentences = 0
    with input_file.open("r", encoding="utf-8") as infile, output_file.open("w", encoding="utf-8") as outfile:
        for idx, line in enumerate(infile, 1):
            if not line.strip():
                continue
            
            try:
                article = json.loads(line)
            except json.JSONDecodeError:
                continue
                
            raw_text = article.get("text", "")
            if not raw_text:
                continue
                
            # Run the NLP pipeline
            processed_sentences = processor.process_article(raw_text)
            
            # Format output for Doccano / Label Studio
            for sent in processed_sentences:
                doccano_record = {
                    "text": sent,
                    "metadata": {
                        "source_id": article.get("id"),
                        "source_url": article.get("url"),
                        "title": article.get("title")
                    }
                }
                outfile.write(json.dumps(doccano_record, ensure_ascii=False) + "\n")
                total_sentences += 1
                
            print(f"Processed article {idx}: Generated {len(processed_sentences)} sentences.")

    print(f"Successfully processed and generated {total_sentences} sentences to {output_file}.")

if __name__ == "__main__":
    main()
