# Tableau Calculated Fields — Streaming Content Analytics

Connect Tableau Desktop to Snowflake:  
`Connect → Snowflake → server: <account>.snowflakecomputing.com`  
Database: `STREAMING_DB`, Schema: `MARTS`  
Tables: `FACT_MOVIES`, `DIM_GENRE`, `DIM_DECADE`

---

## Calculated Fields

```
// Engagement Tier (for color encoding)
IF [Engagement Score] >= 70 THEN "High"
ELSEIF [Engagement Score] >= 40 THEN "Medium"
ELSE "Low"
END

// Avg Rating by Genre (LOD)
{ FIXED [Primary Genre] : AVG([Vote Average]) }

// % of Total Movies (for genre share viz)
COUNTD([Movie Id]) / TOTAL(COUNTD([Movie Id]))

// High-Rated English Movies Flag
IF [Vote Average] >= 7.5 AND [Is English] = TRUE THEN "High-Rated EN"
ELSEIF [Vote Average] >= 7.5 THEN "High-Rated Non-EN"
ELSE "Other"
END

// Genre Count Label
IF [Genre Count] = 1 THEN "Single Genre"
ELSEIF [Genre Count] = 2 THEN "Dual Genre"
ELSE "Multi Genre"
END
```

---

## Suggested Dashboard Sheets

### Sheet 1 — Content Overview
- Bar chart: Movie count by decade (color = avg rating)
- Highlight table: Genre × Decade heatmap (metric = avg engagement)
- KPI text: Total titles, Avg rating, Avg popularity

### Sheet 2 — Genre Intelligence
- Bar chart: Top 10 genres by total votes (audience reach)
- Scatter: Avg popularity vs avg rating per genre (bubble = total movies)
- Treemap: Genre share of catalog

### Sheet 3 — Rating & Popularity Trends
- Line chart: Avg vote average by release year (2000–2024)
- Bar: Top 20 most popular movies (color = rating tier)
- Histogram: Distribution of vote averages

### Sheet 4 — Language & Reach
- Map or bar: Movie count by language (top 10)
- Bar: English vs non-English avg rating comparison
- Scatter: Vote count vs popularity (identify hidden gems: high votes, low popularity)

---

## Dashboard Layout
- Combine all 4 sheets onto one dashboard
- Add genre filter and decade range slider as global filters
- Add language filter (EN / Non-EN toggle)
- Export as `.twbx` (packaged workbook) for GitHub portfolio upload
```
