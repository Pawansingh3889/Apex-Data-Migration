# Phase 3 Results — AI Customer Sentiment Analysis

A locally-hosted LLM reads 500 Portuguese customer reviews and classifies each one as Positive, Neutral, or Negative — without translation, without an API key, and without sending any data to the internet.

---

## The Dataset

The Olist platform has 99,224 customer reviews in total.

| Fact | Number |
|---|---|
| Total reviews | 99,224 |
| Reviews with written text | 40,977 (41%) |
| Star-rating only (no text) | 58,247 (59%) |
| Reviews analysed (sampled) | 500 |

Only reviews with written text can be classified by the LLM. Star-rating-only reviews are excluded.

---

## Why Mistral, Not VADER

The original approach used VADER — a popular English-language sentiment library. It failed completely on this dataset.

| | VADER (dropped) | Mistral 7B via Ollama |
|---|---|---|
| Language | English only | Multilingual — Portuguese native |
| Result on Olist data | 81% Neutral | Meaningful distribution |
| Validated against star ratings | Failed — 5-star reviews scored Neutral | Passes |
| Internet required | No | No |
| Cost | Free | Free |

**VADER's failure:** When VADER encounters a Portuguese word it doesn't know, it assigns a score of 0.0 — which lands as Neutral. Since almost all words in a Portuguese review are unknown to VADER, 81% of all reviews returned Neutral regardless of what they said.

---

## Mistral Results

| Label | Count | Share |
|---|---|---|
| Positive | 291 | 49% |
| Negative | 203 | 34% |
| Neutral | 100 | 17% |

### Validation Against Star Ratings

A reliable model should classify 5-star reviews as mostly Positive, and 1-star reviews as mostly Negative.

| Star rating | Dominant sentiment | Result |
|---|---|---|
| 5 stars | Positive | Pass ✓ |
| 1–2 stars | Negative | Pass ✓ |
| 3 stars | Mixed / Neutral | Expected ✓ |

---

## The Sanitise Fix

Mistral occasionally returns freeform labels instead of the expected `Positive`/`Neutral`/`Negative` contract. Examples caught in production:

- `"Mixed"` — not a valid label
- `"Super recomendo"` — Portuguese phrase, not a label

These were caught by `data_validation.ipynb` before reaching Power BI. Fixed with a `sanitise()` function that maps unexpected outputs via keyword matching:

```python
VALID = {"Positive", "Neutral", "Negative"}

def sanitise(label: str) -> str:
    if label in VALID:
        return label
    label_lower = label.lower()
    if any(w in label_lower for w in ["posit","recommend","bom","recomend"]):
        return "Positive"
    if any(w in label_lower for w in ["negat","bad","ruim","poor"]):
        return "Negative"
    return "Neutral"
```

---

## Category Sentiment — 3-Table Join

Notebook 2 links each review to a product category via a 3-table DuckDB join:

```sql
category_sentiment
  JOIN olist_order_items   ON order_id
  JOIN olist_products      ON product_id
  JOIN product_translation ON category_name
```

Result: 594 review-category pairs across multiple product categories. Each category is scored by:
- Average star rating
- % Positive / Negative / Neutral
- Dominant sentiment label
- High-complaint flag (> 30% Negative)

This gives the business team a direct ranked list of which product lines need attention.

---

## Files Produced

| File | Description |
|---|---|
| `data/enriched_customer_sentiment.csv` | 500 reviews with Mistral sentiment labels |
| `data/category_sentiment.csv` | Per-review category-sentiment join output |
| `data/figures/chart12_13_mistral_sentiment.png` | Sentiment distribution + validation charts |
| `data/figures/chart14_15_category_ratings.png` | Top/bottom categories by avg star rating |
| `data/figures/chart16_most_complained_categories.png` | Categories ranked by complaint rate |
