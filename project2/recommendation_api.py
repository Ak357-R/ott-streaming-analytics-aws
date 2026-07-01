"""
OTT Platform - Content Recommendation API
Phase 2: EC2 Microservice

Deployed on: AWS EC2 t3.micro (eu-north-1 Stockholm)
Server:      Gunicorn (3 workers) managed by systemd
Port:        5000

Endpoints:
  GET /health                              - Health check
  GET /recommend?region=Karnataka&age=25   - Get recommendations
  GET /stats                               - Platform stats

Author: Akash
"""

from flask import Flask, request, jsonify
import random
from datetime import datetime

app = Flask(__name__)

# ── Content Catalog ──────────────────────────────────
CONTENT_BY_GENRE = {
    "Action"      : ["Movie_Title_1","Movie_Title_45","Show_Title_78"],
    "Comedy"      : ["Movie_Title_12","Show_Title_34","Movie_Title_67"],
    "Drama"       : ["Show_Title_5","Movie_Title_23","Show_Title_89"],
    "Thriller"    : ["Movie_Title_8","Show_Title_15","Movie_Title_92"],
    "Romance"     : ["Show_Title_21","Movie_Title_56","Show_Title_43"],
    "Documentary" : ["Movie_Title_33","Show_Title_61","Movie_Title_17"],
    "Sci-Fi"      : ["Movie_Title_72","Show_Title_28","Movie_Title_41"],
    "Horror"      : ["Movie_Title_19","Show_Title_53","Movie_Title_86"]
}

AGE_PREFERENCES = {
    "18-24": ["Action","Sci-Fi","Horror","Comedy"],
    "25-34": ["Thriller","Drama","Action","Comedy"],
    "35-44": ["Drama","Documentary","Romance","Thriller"],
    "45+"  : ["Documentary","Drama","Romance","Comedy"]
}

REGION_LANGUAGES = {
    "Karnataka"   : ["Kannada","Hindi","English"],
    "Tamil Nadu"  : ["Tamil","Hindi","English"],
    "Maharashtra" : ["Marathi","Hindi","English"],
    "Delhi"       : ["Hindi","English"],
    "Kerala"      : ["Malayalam","Hindi","English"],
    "Telangana"   : ["Telugu","Hindi","English"],
    "default"     : ["Hindi","English"]
}

def get_age_group(age):
    if age < 25:   return "18-24"
    elif age < 35: return "25-34"
    elif age < 45: return "35-44"
    else:          return "45+"


# ── Health Check ─────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status"   : "healthy",
        "service"  : "OTT Recommendation API",
        "version"  : "1.0.0",
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    })


# ── Recommendations ───────────────────────────────────
@app.route("/recommend", methods=["GET"])
def recommend():
    region = request.args.get("region", "default")
    age    = int(request.args.get("age", 25))
    limit  = int(request.args.get("limit", 5))

    age_group        = get_age_group(age)
    preferred_genres = AGE_PREFERENCES.get(age_group, ["Action","Drama"])
    languages        = REGION_LANGUAGES.get(region, REGION_LANGUAGES["default"])

    recommendations = []
    for genre in preferred_genres[:3]:
        for content in CONTENT_BY_GENRE.get(genre, [])[:2]:
            recommendations.append({
                "content_id" : f"CNT{random.randint(1,1000):04d}",
                "title"      : content,
                "genre"      : genre,
                "language"   : random.choice(languages),
                "match_score": round(random.uniform(0.75, 0.99), 2)
            })

    recommendations = sorted(
        recommendations, key=lambda x: x["match_score"], reverse=True
    )[:limit]

    return jsonify({
        "user": {
            "region"   : region,
            "age"      : age,
            "age_group": age_group
        },
        "recommendations"  : recommendations,
        "total_recommended": len(recommendations),
        "generated_at"     : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    })


# ── Stats ─────────────────────────────────────────────
@app.route("/stats", methods=["GET"])
def stats():
    return jsonify({
        "total_genres"     : len(CONTENT_BY_GENRE),
        "total_content"    : sum(len(v) for v in CONTENT_BY_GENRE.values()),
        "genres"           : list(CONTENT_BY_GENRE.keys()),
        "regions_supported": list(REGION_LANGUAGES.keys())
    })


if __name__ == "__main__":
    print("Starting OTT Recommendation API...")
    app.run(host="0.0.0.0", port=5000, debug=True)
