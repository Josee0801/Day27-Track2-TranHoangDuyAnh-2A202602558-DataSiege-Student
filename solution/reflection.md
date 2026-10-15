# Reflection

**Which fault types were hardest to catch, and why?**

The hardest faults were the ones that still looked normal: small distribution
shifts, training-serving skew, embedding drift, and changes in runtime. The
published limits gave me a starting point, but staying inside them did not mean
an event was healthy. I added median/MAD checks after forty observations, with
separate, bounded histories for each contract, feature view, corpus, and lineage
job. A minimum scale keeps nearly constant measurements from triggering alerts
over tiny changes. Two moderately unusual metrics can trigger an alert together.

Lineage exposed a gap in my first approach. A clean graph could include upstream
dependencies that were absent from the declared inputs, so checking the
declarations alone missed faults. I kept that check and added a learned one:
after three runs, dependencies seen in at least 80% of a job's previous runs
are treated as expected. That caught missing stable edges while allowing extra
valid ones. Schema and type violations, along with orphan outputs, use direct
checks.

Practice looked strong: 50.00 points, all 33 faults caught, no false positives.
Public scored 43.84, with all four pillars rated high. Private was a different
story. The single completed run scored 27.78 and caught 55.56% of faults, with
no false positives or budget overage. The conservative thresholds missed too
many subtle faults. Some entities may not collect forty observations, and faulty
observations can distort the learned baseline. I left the defense unchanged
after that result and did not rerun private.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

I chose to inspect every event once. Batches and lineage cost 1 credit each,
contracts 1.5, and features or embeddings 2. Practice used 180 of 220 credits;
public used 240 of 220 and took an overage penalty. Private stayed within budget.
This policy checks every arrival, but it cannot guarantee a spending cap. When
a tool result is unavailable, the defense stays quiet and records zero confidence.

On another pass, I would first test shorter warm-ups and more sensitive rules
that combine several signals, using fresh practice streams and tracking false
positives alongside recall. I would also compare checking every event with
sampling expensive checks to leave credits for later arrivals. Given the private
result, improving sensitivity would come before cutting inspection coverage.

I ran the supplied selfcheck, synthetic clean/fault/tool-error cases for all five
handlers, and checks for history limits, entity separation, and missing edges.
All three phases ran through the harness. Getting there required Python 3.12 in
a separate temporary environment to fix Windows child-process startup failures.
I also replaced the statistics import with a local median function because one
of its dependencies was blocked by the assignment sandbox.
