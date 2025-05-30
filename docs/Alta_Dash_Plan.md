# **Alta Dash Plan**

## Executive Overview

### KPI Cards
- **DSO**
- **CEI**
- **Total AR**
- **% > 60 Days**
- **Received MTD**

### Waterfall
- Beginning AR → Sales → Collected → Write-off → Ending AR

---

## AR Health

- **Aging Heat-map**  
  Matrix: age bucket × branch

- **DSO Trend Line**  
  12-mo rolling with service-type slicer

- **Drill-down**  
  Invoice-level

---

## Revenue Leakage

- **Decline-rate Funnel**  
  Attempt → Decline → Retry Success

- **Coupon Impact Bar**  
  Gross vs Net revenue by service-type

- **Leakage % of Sales**  
  (Declines + Coupons + Write-offs) ÷ Gross Sales

---

## Agency Stats

- **Total Placed**  
  (dollars, # accounts)

- **Total Recovered**  
  (dollars, # accounts)

- **Recovery %**  
  (dollars; rolling 4–6 weeks, Y/Y comparison)

- **Net-back After Fees**

---

## AR Ops Health

### Rep Productivity Table(s)  
Import from existing dash?

- Dials/Hour  
- Contact % (contacts / attempts)  
- Contacts/Hour  
- Total Collected  
- Collected/Hour  
- AutoPay % of Payments (autopay / payments)

---

### Program Performance

- Average Collected (Daily, Weekly, Monthly)  
- Collected % of Available  
- Variable ROI (Collected / Wages)  
- Total ROI (Collected / Wages + OH)

---

## [Optional / Ideal] Analytics / Insights

- **Anomaly Detection**  
  E.g., sudden spikes in declines or DSO  
  → Email, Slack alerts?

- **Key Influences**  
  Identify primary drivers of >60-day aging

- **Decomposition Tree**  
  Root-cause of bad debt by branch

- **AutoML Forecast**  
  Predict next-30-day collections

---

## Data Quality Flags

- Bad Address  
- Bad Phone #  
- Bad / No Email  
- Decline Outlier  
- Age Outlier  
- Active but unserviced for 90+ days  
- Account cycling (start, receive service(s), doesn’t pay, cancels, next year starts again)

---

## Layout Principles

- KPI Cards: Top row  
- Interactive Slicers: Right rail (Date [month], Branch, Service Type)  
- Bookmark Buttons: Summary / Detail views (where applicable)  
- Data Definitions: For clarity in calculations
