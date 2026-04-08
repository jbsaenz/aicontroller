# Synthetic Telemetry Data Dictionary

## Entity and Time
- `timestamp` (datetime): observation time.
- `miner_id` (string): unique miner identifier.
- `operating_mode` (category): `eco`, `normal`, `turbo`.

## Environment and Cooling
- `ambient_temperature_c` (float): facility ambient air temperature in Celsius.
- `cooling_power_w` (float): power consumed by cooling subsystem for miner context.

## ASIC Electrical and Performance
- `asic_clock_mhz` (float): chip clock frequency in MHz.
- `asic_voltage_v` (float): chip voltage in volts.
- `asic_hashrate_ths` (float): hashrate in TH/s.
- `asic_temperature_c` (float): chip temperature in Celsius.
- `asic_power_w` (float): ASIC electrical draw in watts.
- `efficiency_j_per_th` (float): joules per terahash.

## Stability and Behavior
- `power_instability_index` (float): normalized indicator [0,1] where higher means unstable power.
- `hashrate_deviation_pct` (float): deviation from miner/mode baseline hashrate percentage.

## Supervised Target
- `failure_within_horizon` (int): binary label (1=expected failure/degradation within prediction horizon, 0=otherwise).
