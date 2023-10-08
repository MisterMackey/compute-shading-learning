# goal
Creating a hardware accellerated program that computes the output of a simple mortgage model over a portfolio

# description of data (input)
schema:
| Id | Notional | Interest Rate | Start Date | Interest Type | 
| Column | Description |
|--------|-------------|
| Id	 | Unique identifier of contract |
| Notional | Original notional value |
| Interest Rate | The current interest rate expressed in annual rate, monthly accrual |
| Start Date | datetime field indicating the start of the contract |
| Interest Type | Enumeration indicating when interest rates reset. Values: { Fixed / Var20 / Var15 / Var10 / Var9 / Var7 / Var5 }
| Term | Duration in years of the contract |
| Remaining Notional | Remaining amount on the loan |
| Payment Type | Enumeration indicating repayment type. Values: { Annuity / Linear / Bullet }
| Risk indicator | Number between 0 and 5 that indicates the probability of defaulting on the next payment, 5 implies a full default and write off of the mortgage with no recovery |
| Next Reset Date | datetime field indicating the next time the interest rate will be re-determined

Risk indicator may change from period to period based on a set of fixed probabilities given below:

| Risk | Chance to move up | Chance to move down |
|------|-------------------|---------------------|
| 0    |  N/A              | 0.1%                |
| 1    |  10%              | 1.0%                |
| 2    |  5%               | 1.0%                |
| 3    |  5%               | 5.0%                |
| 4    |  20%              | 10%                 |
| 5    |  N/A              | N/A                 |

Implied base rate: this is the interest rate minux the additional spread imposed by risk indicator and the reset rate (see generate_data.py for the tables)

# description of output
Some form of overview that shows the incoming payments over the next 30 years, split between repayment and interest.
Extra output showing defaulting rates, defaulting losses etc that can be drilled down by Rate, Term, Type.

# description of model
The solution should simply project expected cashflows over a 30 year horizon, reported at monthly intervals. This means generating a timeseries for each contract recording its payments.
Write-offs are reported seperately, also per monthly period.

### description for the timeseries
For each contract, the payments and writeoffs will be calculated for each monthly period as follows:
- Repayment: fixed amount for linear contracts, annuity payment - interest for annuities, 0 for interest only.
- Interest Payment: monthly rate times remaining notional
- Write off: 100% of the remaining notional if risk indicator reaches 5

The contract details in period T+1 are calculated as follows:
- Remaining notional: Remaining notional in T minus repayment and/or writeoff (if any)
- Interest Rate: if it resets in T+1, the new rate will be the same implied base rate + spreads as per the risk indicator in T and new interest type in T+1
- Interest Type: if interest rate resets in T+1, the new type will be the lowest type that covers the remaining term, unless the original type is smaller than the term remaining ie:
  - A var5 contract with 20 years remaining will remain a var5 contract
  - A var20 contract with 10 years remaining will become a var10 contract
  - A var20 contract with 8 years remaining will become a var9 contract
- Risk indicator: draw the next number from a (pseudo-)random generator to determine if it should change. Probabilities are in the table above

nothing else changes

# solution types that will be benchmarked (i think):
- GLSL + Vulkan
- Python + PySpark
- DotNet 8 (currently in preview)

# what matters
Create the model in both vulkan and some random CPU language (C# i guess) and benchmark it to the tits. The output doesn't matter (as long as it matches) but the speed difference does.

# Dependencies
I'll probably end up missing a few but:
- arrow
- vulkan
- cmake
- gcc/clang
- python >= 3.10 for data generation