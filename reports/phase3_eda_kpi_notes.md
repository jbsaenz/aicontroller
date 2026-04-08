# Phase 3 - EDA and True Efficiency KPI

## Implemented Modules
- `src/kpi.py`
  - TE KPI computation with cooling, voltage, environment, and mode stress factors.
  - KPI summary generation and export.
- `src/eda.py`
  - correlation matrix generation
  - trade-off summary by operating mode
  - anomaly detection using multi-signal stress rules
  - EDA summary export
- `src/visualization.py`
  - correlation heatmap
  - power-vs-hashrate mode scatter
  - TE distribution by operating mode

## TE KPI Formula
- `P_total = asic_power_w + cooling_power_w`
- `BaseEff = asic_hashrate_ths / P_total`
- `V_stress = 1 + alpha_v * max(0, (asic_voltage_v - V_ref)/V_ref)`
- `E_stress = 1 + alpha_e * max(0, (ambient_temperature_c - T_ref)/10)`
- `M_stress = {eco:0.97, normal:1.00, turbo:1.08}`
- `TE = BaseEff / (V_stress * E_stress * M_stress)`

## Current Results Snapshot
- Rows analyzed: 145,152
- TE mean: ~0.0364
- TE by mode (avg): eco > normal > turbo
- Anomaly rows flagged: 399

## Assignment Alignment
- Tier 1 requirement satisfied in this phase:
  - correlations and trade-offs analyzed
  - anomalies identified
  - True Efficiency KPI designed and implemented
