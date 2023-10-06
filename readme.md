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

Risk indicator may change from period to period based on a set of fixed probabilities given below:

| Risk | Chance to move up | Chance to move down |
|------|-------------------|---------------------|
| 0    |  N/A              | 0.1%                |
| 1    |  10%              | 1.0%                |
| 2    |  5%               | 1.0%                |
| 3    |  5%               | 5.0%                |
| 4    |  20%              | 10%                 |
| 5    |  N/A              | N/A                 |


# description of output
Some form of overview that shows the incoming payments over the next 30 years, split between repayment and interest.
Extra output showing defaulting rates, defaulting losses etc that can be drilled down by Rate, Term, Type.

# description of model

# what matters
Create the model in both vulkan and some random CPU language (C# i guess) and benchmark it to the tits. The output doesn't matter (as long as it matches) but the speed difference does.

# Dependencies
I'll probably end up missing a few but:
- arrow
- vulkan
- cmake
- gcc/clang
- python >= 3.10 for data generation