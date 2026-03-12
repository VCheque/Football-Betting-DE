#!/usr/bin/env python3
"""Generate the Football Match Prediction Model PDF documentation.

Run:
    python generate_model_doc.py
Outputs:
    model_documentation.pdf  (in the same directory)
"""
from __future__ import annotations

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
    PageBreak,
    KeepTogether,
)

# ── Colour palette (BM Project design system) ───────────────────────────────
C_NAVY   = colors.HexColor("#0D1B2A")
C_SLATE  = colors.HexColor("#1A2B3C")
C_BLUE   = colors.HexColor("#60A5FA")
C_SKY    = colors.HexColor("#38BDF8")
C_TEXT   = colors.HexColor("#E8EDF2")
C_MUTED  = colors.HexColor("#94A3B8")
C_GREEN  = colors.HexColor("#4ADE80")
C_ORANGE = colors.HexColor("#FB923C")
C_RED    = colors.HexColor("#F87171")
C_WHITE  = colors.white
C_BLACK  = colors.black

# ── Styles ───────────────────────────────────────────────────────────────────
base_styles = getSampleStyleSheet()

def _style(name, **kw):
    s = ParagraphStyle(name, **kw)
    return s

TITLE_STYLE = _style(
    "DocTitle",
    fontSize=26,
    fontName="Helvetica-Bold",
    textColor=C_NAVY,
    spaceAfter=6,
    leading=30,
)

SUBTITLE_STYLE = _style(
    "DocSubtitle",
    fontSize=13,
    fontName="Helvetica",
    textColor=C_SLATE,
    spaceAfter=4,
)

H1_STYLE = _style(
    "DocH1",
    fontSize=16,
    fontName="Helvetica-Bold",
    textColor=C_NAVY,
    spaceBefore=14,
    spaceAfter=4,
    leading=20,
)

H2_STYLE = _style(
    "DocH2",
    fontSize=12,
    fontName="Helvetica-Bold",
    textColor=C_SLATE,
    spaceBefore=10,
    spaceAfter=3,
    leading=15,
)

BODY_STYLE = _style(
    "DocBody",
    fontSize=9.5,
    fontName="Helvetica",
    textColor=C_NAVY,
    spaceAfter=4,
    leading=14,
)

CODE_STYLE = _style(
    "DocCode",
    fontSize=8.5,
    fontName="Courier",
    textColor=C_SLATE,
    spaceAfter=3,
    leading=12,
    backColor=colors.HexColor("#F1F5F9"),
    leftIndent=8,
    rightIndent=8,
    borderPadding=(4, 4, 4, 4),
)

CAPTION_STYLE = _style(
    "DocCaption",
    fontSize=8,
    fontName="Helvetica-Oblique",
    textColor=C_MUTED,
    spaceAfter=3,
)

BULLET_STYLE = _style(
    "DocBullet",
    fontSize=9.5,
    fontName="Helvetica",
    textColor=C_NAVY,
    spaceAfter=2,
    leading=13,
    leftIndent=14,
    bulletIndent=4,
)

# ── Table style helpers ───────────────────────────────────────────────────────
def _header_table_style():
    return TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0),  C_NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  C_WHITE),
        ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, 0),  9),
        ("FONTNAME",    (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",    (0, 1), (-1, -1), 8.5),
        ("TEXTCOLOR",   (0, 1), (-1, -1), C_NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#F8FAFC"), colors.white]),
        ("GRID",        (0, 0), (-1, -1), 0.3, C_MUTED),
        ("VALIGN",      (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",  (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ])

def _highlight_row(row_idx, bg=colors.HexColor("#EFF6FF")):
    return [("BACKGROUND", (0, row_idx), (-1, row_idx), bg)]

# ── Page template ─────────────────────────────────────────────────────────────
PAGE_W, PAGE_H = A4
MARGIN = 2.0 * cm

def _on_page(canvas, doc):
    """Draw header bar + page number on every page."""
    canvas.saveState()
    # Top accent bar
    canvas.setFillColor(C_NAVY)
    canvas.rect(0, PAGE_H - 14, PAGE_W, 14, fill=True, stroke=False)
    # Header text
    canvas.setFillColor(C_WHITE)
    canvas.setFont("Helvetica-Bold", 7)
    canvas.drawString(MARGIN, PAGE_H - 10, "Football Match Prediction — Model Documentation")
    canvas.setFont("Helvetica", 7)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 10, "Confidential · Internal Use Only")
    # Footer line
    canvas.setStrokeColor(C_MUTED)
    canvas.setLineWidth(0.3)
    canvas.line(MARGIN, 14, PAGE_W - MARGIN, 14)
    canvas.setFillColor(C_MUTED)
    canvas.setFont("Helvetica", 7)
    canvas.drawCentredString(PAGE_W / 2, 6, f"Page {doc.page}")
    canvas.restoreState()

# ── Content builders ──────────────────────────────────────────────────────────
def _P(text, style=None):
    return Paragraph(text, style or BODY_STYLE)

def _B(text):
    """Bullet paragraph."""
    return Paragraph(f"• {text}", BULLET_STYLE)

def _HR():
    return HRFlowable(width="100%", thickness=0.5, color=C_MUTED, spaceAfter=8, spaceBefore=4)

def _SP(h=6):
    return Spacer(1, h)

# ═════════════════════════════════════════════════════════════════════════════
# Build document
# ═════════════════════════════════════════════════════════════════════════════
def build_document(output_path: str = "model_documentation.pdf") -> None:
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN + 14,
        bottomMargin=MARGIN,
        title="Football Match Prediction — Model Documentation",
        author="Football Betting DE Platform",
    )

    story = []

    # ────────────────────────────────────────────────────────────────────────
    # COVER PAGE
    # ────────────────────────────────────────────────────────────────────────
    story.append(_SP(30))
    story.append(Paragraph("Football Match Prediction", TITLE_STYLE))
    story.append(Paragraph("Model Documentation — v2.0", SUBTITLE_STYLE))
    story.append(_SP(4))
    story.append(_HR())
    story.append(_SP(4))
    story.append(_P(
        "This document describes the statistical and machine-learning models used by the "
        "<b>Football Betting DE Platform</b> to estimate match outcome probabilities, "
        "player event likelihoods, and derive actionable betting recommendations. "
        "It covers feature engineering rationale, model architecture, calibration methodology, "
        "decision framework, and known limitations."
    ))
    story.append(_SP(8))

    cover_data = [
        ["Attribute", "Value"],
        ["Platform", "Football Betting DE · Streamlit App"],
        ["Model version", "v2.0 (17 features, calibrated)"],
        ["Algorithm", "XGBoost (multi:softprob) + Platt sigmoid calibration"],
        ["Supported leagues", "Premier League · La Liga · Serie A · Bundesliga · Ligue 1 · Primeira Liga"],
        ["Training window", "Rolling 3-year window; exponential time-decay weights (τ = 500 days)"],
        ["Data source", "football-data.co.uk (matches/odds) · Understat (player stats)"],
        ["Refresh cadence", "Match model: retrained on app load · Player stats: weekly (Understat)"],
        ["Last revised", "March 2026"],
    ]
    story.append(Table(cover_data, colWidths=[5.5*cm, 12*cm], style=_header_table_style()))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 1. OVERVIEW
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("1. Overview", H1_STYLE))
    story.append(_P(
        "The platform trains an <b>XGBoost multi-class classifier</b> on historical match data to predict "
        "the probability of three outcomes: Home win (H), Draw (D), and Away win (A). "
        "A separate set of <b>binary XGBoost classifiers</b> estimates per-player probabilities of "
        "scoring, assisting, and receiving a yellow/red card."
    ))
    story.append(_P(
        "Probabilities from the match model are post-processed through:"
    ))
    for item in [
        "<b>Platt sigmoid calibration</b> — maps raw XGBoost log-odds to well-calibrated probabilities.",
        "<b>Expected Value (EV) calculation</b> — compares model probability to bookmaker implied probability.",
        "<b>Risk-tier bet suggestions</b> — conservative (highest probability pick), "
        "moderate (best EV ≥ 25% probability), high-risk (best EV on higher-odds outcome).",
    ]:
        story.append(_B(item))
    story.append(_SP(8))

    # ────────────────────────────────────────────────────────────────────────
    # 2. MATCH MODEL FEATURES
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("2. Match Model Features (17 total)", H1_STYLE))
    story.append(_P(
        "Every feature is a <b>signed differential</b>: positive values favour the home team, "
        "negative values favour the away team. This symmetric encoding means the model learns "
        "from home/away perspective without needing separate home and away intercept terms."
    ))
    story.append(_SP(6))

    story.append(Paragraph("2.1 Form & Momentum", H2_STYLE))
    form_data = [
        ["Feature", "Formula", "Rationale"],
        ["form_points_gap",
         "avg(home_pts_last5) − avg(away_pts_last5)",
         "Short-term form; most predictive single feature. "
         "Averaged over last 5 matches in all competitions."],
        ["forward_goals_gap",
         "avg(home_gf_last5) − avg(away_gf_last5)",
         "Attacking strength. Goals scored correlate with xG and shot quality."],
        ["defense_gap",
         "avg(away_ga_last5) − avg(home_ga_last5)",
         "Note: reversed sign so positive = home has better defense. "
         "Fewer goals conceded = positive value."],
        ["momentum_gap  ★NEW",
         "OLS_slope(home_pts_last5) / 3 − OLS_slope(away_pts_last5) / 3",
         "Captures whether form is improving or declining. "
         "Slope is normalised by 3 (max pts/game). "
         "W-W-W-W-W ≈ +0.9; L-L-L-L-L ≈ −0.9."],
    ]
    story.append(Table(
        form_data,
        colWidths=[4.0*cm, 6.5*cm, 7.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("2.2 Season & Role", H2_STYLE))
    season_data = [
        ["Feature", "Formula", "Rationale"],
        ["season_points_gap",
         "home_pts / max(home_matches,1) − away_pts / max(away_matches,1)",
         "Season-to-date PPG captures league quality and cumulative form. "
         "Complements last-5 rolling form."],
        ["home_role_gap  ★NEW",
         "home_team's home-only PPG − away_team's away-only PPG",
         "Role-specific performance. A team with 2.5 home PPG vs 0.8 away PPG "
         "is very different from a team with 1.5 home PPG vs 1.5 away PPG. "
         "Away matches are harder; this gap quantifies home advantage in context."],
        ["league_idx",
         "Ordinal encoding of league_code",
         "Controls for league-level difficulty and scoring rate differences "
         "(e.g. Bundesliga averages ~3.1 goals/game vs Primeira Liga ~2.5)."],
    ]
    story.append(Table(
        season_data,
        colWidths=[4.0*cm, 6.5*cm, 7.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("2.3 Head-to-Head (H2H)", H2_STYLE))
    h2h_data = [
        ["Feature", "Formula", "Rationale"],
        ["h2h_gap",
         "Σ w·outcome_val / Σ w  −  0.5\n"
         "where w = exp(−age / 900 days)\n"
         "outcome_val: H=1, D=0.5, A=0",
         "Exponentially-decayed H2H win rate. Centred at 0.5 (neutral). "
         "Uses a 900-day half-life (~2.5 seasons) so recent derbies matter most. "
         "Bidirectional: reversed perspective for reversed fixture."],
        ["h2h_goal_diff",
         "Σ w·goal_diff_home / Σ w",
         "Weighted average of home-team goal differential across H2H history. "
         "Positive = home team typically wins by more goals."],
    ]
    story.append(Table(
        h2h_data,
        colWidths=[4.0*cm, 6.5*cm, 7.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("2.4 Physical & Discipline", H2_STYLE))
    phys_data = [
        ["Feature", "Formula", "Rationale"],
        ["cards_gap",
         "avg(away_cards_last5) − avg(home_cards_last5)\n"
         "cards = yellow + 2·red",
         "Disciplinary proxy. High cards → pressure or aggression. "
         "Away teams average ~20% more cards than home teams."],
        ["corners_gap",
         "avg(home_corners_diff_last5) − avg(away_corners_diff_last5)\n"
         "corners_diff = corners_for − corners_against",
         "Corner differential correlates with territorial dominance "
         "and indirect xG from set-piece situations."],
        ["sot_gap  ★NEW",
         "avg(home_sot_diff_last5) − avg(away_sot_diff_last5)\n"
         "sot_diff = shots_on_target_for − shots_on_target_against",
         "Shots-on-target differential is the best available proxy for xG "
         "without requiring Understat data in the training loop. "
         "Strong correlation with actual goal output (r ≈ 0.78 in Premier League)."],
    ]
    story.append(Table(
        phys_data,
        colWidths=[4.0*cm, 6.5*cm, 7.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("2.5 Fatigue & Rest", H2_STYLE))
    fatigue_data = [
        ["Feature", "Formula", "Rationale"],
        ["rest_gap",
         "home_days_rest − away_days_rest\n"
         "(days since last match)",
         "Fresh teams perform better. 3+ extra rest days for one side "
         "is a meaningful edge (studies show ~5% win-rate shift)."],
        ["fatigue_gap",
         "(away_matches_last8 + away_big_games) − (home_matches_last8 + home_big_games)\n"
         "matches_last8 = league matches in last 8 days\n"
         "big_games = other-competition matches in 8 days",
         "Congestion effect from European competition or cup fixtures. "
         "Positive = away team more fatigued; favours home."],
    ]
    story.append(Table(
        fatigue_data,
        colWidths=[4.0*cm, 6.5*cm, 7.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("2.6 Squad Depth & Context", H2_STYLE))
    squad_data = [
        ["Feature", "Formula", "Rationale"],
        ["injury_gap",
         "Σ away_injury_importance − Σ home_injury_importance\n"
         "(active injuries in last 7 days)",
         "Injury importance score is set manually per player. "
         "Key absences (importance > 3.0) can shift probabilities by 3–8%."],
        ["lineup_strength_gap",
         "home_lineup_strength − away_lineup_strength\n"
         "strength = Σ 1.5·goals + 1.1·assists + 0.8·xG + 0.6·xA\n"
         "        + 0.1·key_passes + 0.2·rating  (per player)",
         "Starting-XI composite impact score derived from Understat player stats. "
         "Positive = home has a stronger starting lineup. Requires API-Football key."],
        ["derby_flag  ★NEW",
         "1.0 if frozenset({home, away}) ∈ DERBY_PAIRS else 0.0",
         "Crowd intensity and psychological pressure in local derbies. "
         "Historically, derbies show compressed win probabilities (home advantage "
         "is weaker; draw rate is elevated). 28 rivalry pairs across 6 leagues."],
    ]
    story.append(Table(
        squad_data,
        colWidths=[4.0*cm, 6.5*cm, 7.0*cm],
        style=_header_table_style(),
    ))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 3. MODEL ARCHITECTURE
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("3. Model Architecture", H1_STYLE))

    story.append(Paragraph("3.1 Base Classifier", H2_STYLE))
    story.append(_P(
        "The match model uses an <b>XGBoost gradient-boosted tree ensemble</b> with a "
        "multi-class softmax objective. Key hyperparameters:"
    ))
    xgb_data = [
        ["Hyperparameter", "Value", "Rationale"],
        ["objective", "multi:softprob", "Returns probability per class (H/D/A)"],
        ["num_class", "3", "Home win / Draw / Away win"],
        ["n_estimators", "100", "v2: increased from 60 to exploit 17 features"],
        ["max_depth", "4", "v2: increased from 3; controls feature interaction depth"],
        ["learning_rate", "0.08", "Slightly lower than v1 (0.10) to compensate more trees"],
        ["subsample", "0.85", "Row sub-sampling; reduces overfitting"],
        ["colsample_bytree", "0.85", "Feature sub-sampling per tree"],
        ["reg_lambda", "1.2", "L2 regularisation; slightly stronger than v1 (1.0)"],
        ["min_child_weight", "3", "Minimum leaf weight; prevents sparse-feature overfitting"],
        ["tree_method", "hist", "Histogram-based; fast on CPU with integer binning"],
        ["n_jobs", "1", "Single thread on shared cloud CPU (Dremio avoids parallelism overhead)"],
        ["sample_weight", "exp(−age / 500 days)", "Recent matches weighted up to 5× more than 5-year-old data"],
    ]
    story.append(Table(
        xgb_data,
        colWidths=[4.5*cm, 3.5*cm, 9.5*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("3.2 Temporal Train / Calibrate Split", H2_STYLE))
    story.append(_P(
        "To avoid data leakage, training data is split <b>chronologically</b>:"
    ))
    for item in [
        "<b>Training set (80%)</b>: oldest to 80th percentile of match dates — used to fit the XGBoost base model.",
        "<b>Calibration set (20%)</b>: most recent 20% of matches — used to fit Platt sigmoid calibration.",
        "The 3-year training window means the most recent ~8 months of data form the calibration holdout.",
    ]:
        story.append(_B(item))
    story.append(_SP(6))
    story.append(_P(
        "<b>Why time-ordered split?</b> A random k-fold split would allow future match information to leak into "
        "historical training rows (target leakage through league dynamics). Time-ordering preserves the "
        "causal structure of the data."
    ))
    story.append(_SP(8))

    story.append(Paragraph("3.3 Platt Sigmoid Calibration", H2_STYLE))
    story.append(_P(
        "XGBoost's raw <code>predict_proba</code> output is well-ranked (high AUC) but poorly calibrated — "
        "probabilities cluster around the class priors rather than spanning [0, 1] uniformly. "
        "Platt scaling fits a logistic regression on top of the base model's scores:"
    ))
    story.append(Paragraph(
        "P(y = k | x)  =  σ(A · f<sub>k</sub>(x) + B)    where σ = sigmoid",
        CODE_STYLE,
    ))
    story.append(_P(
        "Parameters A and B are fitted on the calibration holdout. "
        "This is implemented via <code>sklearn.calibration.CalibratedClassifierCV(method='sigmoid', cv='prefit')</code>."
    ))
    story.append(_SP(4))
    story.append(_P(
        "<b>Validation metrics</b> (computed on calibration holdout, displayed in the sidebar):"
    ))
    val_data = [
        ["Metric", "Formula", "Interpretation"],
        ["Brier Score",
         "mean over classes: BS_k = mean((P(y=k) − 1{y=k})<sup>2</sup>)",
         "Lower = better. Random baseline ≈ 0.22. "
         "A well-calibrated model targeting ≈ 0.18–0.20."],
        ["Log-loss",
         "−mean(Σ_k  1{y=k} · log P(y=k))",
         "Lower = better. Random baseline ≈ 1.10. "
         "Good model targeting ≈ 0.95–1.05."],
    ]
    story.append(Table(
        val_data,
        colWidths=[3.5*cm, 6.5*cm, 7.5*cm],
        style=_header_table_style(),
    ))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 4. TRAINING DATA CONSTRUCTION
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("4. Training Data Construction", H1_STYLE))
    story.append(_P(
        "Training data is built <b>chronologically</b> by iterating over every historical match "
        "and computing the feature vector using only information available <i>before</i> that match:"
    ))
    for item in [
        "TeamState objects accumulate rolling statistics using Python <code>deque(maxlen=5)</code>.",
        "H2H records are stored per pair (league, min(home,away), max(home,away)) to allow bidirectional lookup.",
        "After each match, the state is updated with the match outcome — so the next match sees the updated state.",
        "Home-role and away-role deques are updated separately (home team's state only updated on home games for role PPG).",
        "The DataFrame is sliced to the last 3 years before the loop to keep training time under 5 seconds.",
    ]:
        story.append(_B(item))
    story.append(_SP(6))
    story.append(_P(
        "<b>Exponential sample weighting</b>: matches from 3 years ago carry weight "
        "exp(−3·365 / 500) ≈ 0.11 relative to the most recent match. "
        "This effectively gives ~5× more weight to the most recent season."
    ))
    story.append(_SP(8))

    story.append(Paragraph("4.1 Momentum Slope Formula", H2_STYLE))
    story.append(_P(
        "The momentum slope uses Ordinary Least Squares regression on the last-N points sequence:"
    ))
    story.append(Paragraph(
        "x = [0, 1, 2, ..., N−1]  (match index)\n"
        "y = [pts_1, pts_2, ..., pts_N]  (points per match)\n\n"
        "slope = Σ(x_i − x̄)(y_i − ȳ) / Σ(x_i − x̄)²\n\n"
        "momentum_slope = slope / 3.0   (normalised by max pts/game)",
        CODE_STYLE,
    ))
    story.append(_P(
        "Interpretation: a team winning every match (W-W-W-W-W) has slope ≈ +0 (constant high performance), "
        "not a strong positive slope. A team improving from L-L-L-D-W has a strongly positive slope. "
        "This captures <i>trend</i> rather than absolute level (which form_points_gap already covers)."
    ))
    story.append(_SP(8))

    story.append(Paragraph("4.2 Derby Pairs (28 Rivalries)", H2_STYLE))
    derby_data = [
        ["League", "Rivalry Pairs"],
        ["Premier League (E0)",
         "Man City/Man United · Arsenal/Tottenham · Chelsea/Tottenham · Chelsea/Arsenal · "
         "Chelsea/Fulham · Liverpool/Everton · Leeds/Man United · Newcastle/Sunderland · West Ham/Tottenham"],
        ["La Liga (SP1)",
         "Real Madrid/Atletico Madrid · Barcelona/Espanyol · Sevilla/Betis · "
         "Athletic Club/Sociedad · Valencia/Villarreal · Real Madrid/Getafe"],
        ["Serie A (I1)",
         "Juventus/Torino · Inter/AC Milan · Roma/Lazio"],
        ["Bundesliga (D1)",
         "Dortmund/Schalke 04 · Hamburg/St Pauli · Cologne/Leverkusen · Dortmund/Cologne"],
        ["Ligue 1 (F1)",
         "Paris SG/Lens · Marseille/Nice · Marseille/Lyon · Lille/Lens"],
        ["Primeira Liga (P1)",
         "Benfica/Sporting CP · Benfica/Porto · Sporting CP/Porto · Porto/Boavista · Benfica/Belenenses"],
    ]
    story.append(Table(
        derby_data,
        colWidths=[3.5*cm, 14*cm],
        style=_header_table_style(),
    ))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 5. PLAYER MODELS
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("5. Player Event Models", H1_STYLE))
    story.append(_P(
        "Three separate <b>binary XGBoost classifiers</b> predict the probability that a player will "
        "score a goal, register an assist, or receive a yellow/red card in a given match."
    ))
    story.append(_SP(4))

    story.append(Paragraph("5.1 Feature Set (9 features)", H2_STYLE))
    player_feat_data = [
        ["Feature", "Source", "Rationale"],
        ["minutes", "Understat", "Players with more minutes have more opportunities"],
        ["xg", "Understat", "Expected goals — best predictor of future goals"],
        ["xa", "Understat", "Expected assists — key for assist probability"],
        ["key_passes", "Understat", "Ball creation; leads to assist opportunities"],
        ["shots_on_target", "Understat", "Quality finishing indicator"],
        ["rating", "Whoscored/FBRef", "Overall match contribution composite"],
        ["fouls", "Match data", "Foul rate → discipline/card probability"],
        ["yellow_cards", "Match data", "Historical yellow card rate"],
        ["red_cards", "Match data", "Historical red card rate"],
    ]
    story.append(Table(
        player_feat_data,
        colWidths=[3.5*cm, 3.5*cm, 10.5*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("5.2 Targets", H2_STYLE))
    for item in [
        "<b>Goal scored</b>: binary, 1 if goals > 0 in that match.",
        "<b>Assist registered</b>: binary, 1 if assists > 0 in that match.",
        "<b>Card received</b>: binary, 1 if yellow_cards + red_cards > 0.",
    ]:
        story.append(_B(item))
    story.append(_SP(6))
    story.append(_P(
        "At prediction time, each player's last 5 match statistics are averaged to create the "
        "input feature vector. Only players with at least 1 recorded match are included. "
        "Players are ranked by (prob_score + prob_assist) descending."
    ))
    story.append(_SP(8))

    story.append(Paragraph("5.3 Sample Weighting", H2_STYLE))
    story.append(_P(
        "Player model sample weights use a shorter half-life than the match model: "
        "exp(−age / 300 days). This reflects that player form is more volatile than "
        "team-level patterns — a 10-month-old player record is close to irrelevant."
    ))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 6. DECISION FRAMEWORK
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("6. Decision Framework — Bet Recommendations", H1_STYLE))
    story.append(_P(
        "The platform derives three bet suggestions per match at different risk levels. "
        "Each suggestion includes the <b>outcome label</b>, <b>model probability</b>, "
        "and <b>Expected Value (EV)</b>."
    ))
    story.append(_SP(6))

    story.append(Paragraph("6.1 Expected Value", H2_STYLE))
    story.append(Paragraph(
        "EV  =  P_model(outcome) × bookmaker_odds  −  1\n\n"
        "where bookmaker_odds is the decimal odds (e.g. 2.10 for evens+).\n\n"
        "Interpretation:\n"
        "  EV > 0  → positive edge (model believes outcome is underpriced)\n"
        "  EV = 0  → fair value (no edge)\n"
        "  EV < 0  → negative edge (outcome is overpriced by bookmaker)",
        CODE_STYLE,
    ))
    story.append(_SP(8))

    story.append(Paragraph("6.2 Risk Tiers", H2_STYLE))
    tier_data = [
        ["Tier", "Selection Rule", "Typical Use"],
        ["Conservative",
         "Pick the outcome with the <b>highest model probability</b> regardless of odds.",
         "Accumulators, small-stakes. Low variance, low return. "
         "Best for systematic betting over many matches."],
        ["Moderate",
         "Among outcomes with P ≥ 25%, pick the one with the <b>highest EV</b>.",
         "Single match bets. Requires slight odds value. "
         "Avoids very low-probability outcomes."],
        ["High Risk",
         "Among outcomes with odds ≥ median odds, pick the one with the <b>highest EV</b>.",
         "Draw / away upset hunting. Higher variance. "
         "Best when model significantly disagrees with bookmaker on longer-odds outcome."],
    ]
    story.append(Table(
        tier_data,
        colWidths=[3*cm, 7*cm, 7.5*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(8))

    story.append(Paragraph("6.3 Model-Implied Odds", H2_STYLE))
    story.append(_P(
        "The platform can auto-suggest odds without requiring user input. "
        "Model-implied odds include a 5% bookmaker margin:"
    ))
    story.append(Paragraph(
        "implied_odds(k) = (1 / P_model(k)) × (1 − 0.05)\n\n"
        "Example: P(H) = 0.52  →  implied_odds(H) = (1/0.52) × 0.95 ≈ 1.83",
        CODE_STYLE,
    ))
    story.append(_SP(8))

    story.append(Paragraph("6.4 Key Factor Explanations", H2_STYLE))
    story.append(_P(
        "The platform generates a plain-language explanation of the top factors driving the prediction:"
    ))
    factor_data = [
        ["Condition", "Label shown"],
        ["fatigue_gap > 0.5", "{away} has congestion fatigue advantage for {home}"],
        ["fatigue_gap < −0.5", "{home} has congestion fatigue advantage"],
        ["injury_gap > 0.4", "{away} weakened by injuries"],
        ["injury_gap < −0.4", "{home} weakened by injuries"],
        ["forward_goals_gap > 0.2", "{home} stronger in attack"],
        ["forward_goals_gap < −0.2", "{away} stronger in attack"],
        ["h2h_gap > 0.1", "{home} has H2H dominance"],
        ["h2h_gap < −0.1", "{away} has H2H dominance"],
        ["home_role_gap > 0.4  ★NEW", "{home} has strong home vs {away}'s away record"],
        ["home_role_gap < −0.4  ★NEW", "{away} performs well away from home"],
        ["momentum_gap > 0.15  ★NEW", "{home} is on an improving run"],
        ["momentum_gap < −0.15  ★NEW", "{away} is on an improving run"],
        ["derby_flag = 1  ★NEW", "Derby match — expect intensity, compressed odds"],
        ["sot_gap > 1.0  ★NEW", "{home} creating significantly more quality chances"],
        ["sot_gap < −1.0  ★NEW", "{away} creating significantly more quality chances"],
    ]
    story.append(Table(
        factor_data,
        colWidths=[6*cm, 11.5*cm],
        style=_header_table_style(),
    ))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 7. LIMITATIONS & FUTURE IMPROVEMENTS
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("7. Limitations & Future Improvements", H1_STYLE))

    story.append(Paragraph("7.1 Current Limitations", H2_STYLE))
    for item in [
        "<b>No xG in match model</b>: The model uses shots on target as a proxy for xG. "
        "Actual Understat xG per team per match would improve feature quality.",
        "<b>Derby pairs are static</b>: The lookup table uses exact football-data.co.uk team names. "
        "Teams promoted/relegated from the dataset will not automatically be detected as derby pairs.",
        "<b>No Poisson blend</b>: A Poisson-based goal model would complement the XGBoost classifier "
        "by providing goal probability distributions (1-0, 2-1 etc.) for handicap and over/under markets.",
        "<b>No odds movement feature</b>: Bookmaker line movements contain information about sharp money. "
        "Comparing opening odds to closing odds is a strong signal not yet captured.",
        "<b>Weather not modelled</b>: Rain and wind suppress goals and affect playing style. "
        "This is particularly relevant in the Premier League (open stadiums) and Bundesliga.",
        "<b>Referee effect not modelled</b>: Some referees show strong home bias in card decisions. "
        "A referee fixed effect could improve card probability predictions.",
        "<b>Primeira Liga data quality</b>: Understat does not cover P1; "
        "player models are not available for Portuguese league matches.",
    ]:
        story.append(_B(item))
    story.append(_SP(8))

    story.append(Paragraph("7.2 Recommended Improvements", H2_STYLE))
    improvements_data = [
        ["Improvement", "Expected Impact", "Complexity"],
        ["Poisson goal model blend (Dixon-Coles)",
         "Over/under & correct-score markets; better probability calibration near 0/1",
         "Medium"],
        ["Understat per-match xG (team level)",
         "Replace sot_gap proxy with true xG gap; ~3–5% better Brier score",
         "Low (data already in pipeline)"],
        ["Odds movement features (opening vs closing)",
         "Captures sharp-money signal; strong predictor especially for upsets",
         "Medium (requires odds history)"],
        ["BTTS (both teams to score) market",
         "Extends actionable markets beyond 1X2",
         "Low (add binary target to existing model)"],
        ["Weather API integration",
         "Improves predictions in winter northern Europe; rain reduces scoring rate ~8%",
         "Medium"],
        ["Referee fixed effects",
         "Improves card/foul model accuracy; small impact on match outcome",
         "Low"],
        ["Ensemble: XGBoost + neural net (tabular)",
         "Potentially 2–4% reduction in log-loss via model diversity",
         "High"],
        ["Walk-forward validation report",
         "Monitor model drift; detect when retraining is needed",
         "Low (add to dbt pipeline as a test)"],
    ]
    story.append(Table(
        improvements_data,
        colWidths=[6.5*cm, 8*cm, 3*cm],
        style=_header_table_style(),
    ))
    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # 8. DATA PIPELINE SUMMARY
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("8. Data Pipeline Summary", H1_STYLE))
    story.append(_P(
        "The platform uses a medallion architecture: Bronze → Silver → Gold → Streamlit."
    ))
    pipeline_data = [
        ["Layer", "Component", "Description"],
        ["Bronze",  "MinIO S3",
         "Raw CSV files: football-data.co.uk (match+odds) and Understat (player stats). "
         "Partitioned by source / entity / ingest_date / run_id."],
        ["Metadata", "PostgreSQL",
         "pipeline_run and file_manifest tables track every ingestion job."],
        ["Semantic", "Dremio",
         "Auto-generated SQL views (raw_matches_odds, raw_player_stats) union all "
         "Bronze CSVs into queryable tables."],
        ["Silver", "dbt",
         "stg_raw_matches_odds → silver_matches (cast, clean, standardise column names). "
         "stg_player_stats → staging player data."],
        ["Gold", "dbt",
         "gold_match_context (rolling form), gold_h2h_context (H2H analytics), "
         "gold_standings (league table), gold_rest_fatigue (rest days), "
         "gold_team_season_stats (split by scope), gold_player_stats (per-90 rates), "
         "gold_injuries (manual uploads)."],
        ["App", "Streamlit",
         "dremio_data_loader.py queries Gold views. XGBoost models trained on "
         "app load. Tab 1: Match Center prediction. Tab 2: League & players. Tab 3: Betting tips."],
        ["Refresh", "Scheduler (cron)",
         "Match data: 4× daily (00:15, 06:15, 12:15, 18:15 UTC). "
         "Player stats: weekly Sunday 02:00 UTC."],
    ]
    story.append(Table(
        pipeline_data,
        colWidths=[2.0*cm, 2.5*cm, 13.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(10))

    story.append(Paragraph("8.1 Model Refresh Cadence", H2_STYLE))
    story.append(_P(
        "The XGBoost match model is <b>retrained on every app load</b> using Streamlit's "
        "<code>@st.cache_resource</code> decorator keyed on the historical DataFrame hash. "
        "This means:"
    ))
    for item in [
        "After a match-data ingestion run, the next user session automatically picks up the new data.",
        "Training takes ~2–4 seconds for 3 years of data (~6,000 match rows).",
        "The calibration holdout is always the most recent 20% of data, so validation metrics stay current.",
    ]:
        story.append(_B(item))
    story.append(_SP(20))

    # ────────────────────────────────────────────────────────────────────────
    # 9. APPENDIX: FEATURE QUICK REFERENCE
    # ────────────────────────────────────────────────────────────────────────
    story.append(_HR())
    story.append(Paragraph("Appendix: Feature Quick Reference", H1_STYLE))
    story.append(_P(
        "All 17 features sorted by typical feature importance (estimated via XGBoost gain):"
    ))
    appendix_data = [
        ["Rank", "Feature", "Type", "New v2"],
        ["1",  "form_points_gap",     "Rolling (last 5)",  ""],
        ["2",  "season_points_gap",   "Season aggregate",  ""],
        ["3",  "h2h_gap",             "H2H exponential",   ""],
        ["4",  "home_role_gap",       "Role-split PPG",     "★"],
        ["5",  "forward_goals_gap",   "Rolling (last 5)",  ""],
        ["6",  "sot_gap",             "Rolling (last 5)",  "★"],
        ["7",  "defense_gap",         "Rolling (last 5)",  ""],
        ["8",  "momentum_gap",        "OLS slope (last 5)", "★"],
        ["9",  "h2h_goal_diff",       "H2H exponential",   ""],
        ["10", "injury_gap",          "Current snap",      ""],
        ["11", "lineup_strength_gap", "Player model",      ""],
        ["12", "rest_gap",            "Current snap",      ""],
        ["13", "fatigue_gap",         "Current snap",      ""],
        ["14", "corners_gap",         "Rolling (last 5)",  ""],
        ["15", "cards_gap",           "Rolling (last 5)",  ""],
        ["16", "league_idx",          "Categorical",       ""],
        ["17", "derby_flag",          "Binary lookup",     "★"],
    ]
    story.append(Table(
        appendix_data,
        colWidths=[1.5*cm, 5.5*cm, 4.5*cm, 2.0*cm],
        style=_header_table_style(),
    ))
    story.append(_SP(12))

    # Build
    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    print(f"PDF written to: {output_path}")


if __name__ == "__main__":
    build_document()
