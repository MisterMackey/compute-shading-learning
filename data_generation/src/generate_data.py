from datetime import datetime, timedelta
from typing import List
import pyarrow.parquet as pq
import pyarrow as pa
import uuid
import random

colNames = ['Id', 'Notional', 'Interest Rate', 'Interest Type', 'Start Date', 'Term', 'Remaining Notional', 'Payment Type', 'Risk Indicator']
additionalInterestRatePerDuration = {
	'Fixed': 2.0,
	'Var20': 1.5,
	'Var15': 1.0,
	'Var10': 0.5,
	'Var9': 0.4,
	'Var7': 0.1,
	'Var5': 0.0,

}
additionalInterestRatePerRiskCategory = {
	0: 0.0,
	1: 0.3 ,
	2: 1.1 ,
	3: 1.9 ,
	4: 3.5 ,
}

ASSUMED_DATE_TODAY=datetime(year=2023,month=1,day=1)

def main():
	data = []
	#example
	data.append(['Id1'])
	data.append([400000])
	data.append([2.0])
	data.append(['Fixed'])
	data.append(['2023-01-01'])
	data.append([30])
	data.append([400000])
	data.append(['Annuity'])
	data.append([0])
	table = pa.table(data, names=colNames)
	
	pq.write_table(table, "test_data.parquet", compression="snappy")

def generate_id(num: int) -> List[str]:
        lst = []
        return lst

def generate_row():
	Id = str(uuid.uuid4())
	Notional = random.randint(100000, 800000)
	Term = random.choice([30, 25, 20])
	InterestType = random.choice(['Fixed', 'Var20', 'Var15', 'Var10', 'Var9', 'Var7', 'Var5'])
	if Term == 20 and InterestType == 'Var20':
		InterestType = 'Fixed'
	RiskIndicator = random.random()
	if RiskIndicator < .8:
		RiskIndicator = 0
	elif RiskIndicator < .9:
		RiskIndicator = 1
	elif RiskIndicator < .95:
		RiskIndicator = 2
	elif RiskIndicator < .98:
		RiskIndicator = 3
	else:
		RiskIndicator = 4
	InterestRisk = random.random() + .5 + additionalInterestRatePerRiskCategory[RiskIndicator] + additionalInterestRatePerDuration[InterestType]
	randomDelta = random.randint(0, 365*25)
	StartDate = datetime.today() - timedelta(days=randomDelta)
	PaymentType = random.choice(['Annuity', 'Linear', 'Bullet'])
	RemainingNotional = calc_remaining_notional(Notional, StartDate, InterestRisk, PaymentType, Term)
 
def calc_remaining_notional(notional: int, start_date: datetime, interest: float, paytype: str, term: int) -> float:
	match paytype:
		case 'Annuity':
			return calc_remaining_annuity(notional, start_date, interest, term)
		case 'Linear':
			return calc_remaining_linear(notional, start_date, term)
		case 'Bullet':
			return float(notional)
		case _:
			raise Exception("bruh moment")

def calc_remaining_annuity(notional: int, start_date: datetime, interest: float, term: int) -> float:
	months = (ASSUMED_DATE_TODAY.year - start_date.year) * 12 + ASSUMED_DATE_TODAY.month - start_date.month
	monthsTotal = term * 12
	r = interest / 12 / 100
	payAmount = (notional*r*(pow(1+r, monthsTotal))) / (pow(1+r, monthsTotal) -1)
	return notional * pow(1+r, months) - payAmount* ((pow(1+r, months) -1) / r)

def calc_remaining_linear(notional: int, start_date: datetime, term: int) -> float:
	months = (ASSUMED_DATE_TODAY.year - start_date.year) * 12 + ASSUMED_DATE_TODAY.month - start_date.month
	monthsTotal = term * 12
	permonth = notional / monthsTotal
	return notional - (months * permonth)

if __name__ == '__main__':
	main()