import pyarrow as pa

colNames = ['Id', 'Notional', 'Interest Rate', 'Interest Type', 'Start Date', 'Term', 'Remaining Notional', 'Payment Type', 'Risk Indicator']

def main():
	data = []
	#example
	data.append([['Id1']])
	data.append([400000])
	data.append([2.0])
	data.append(['Fixed'])
	data.append(['2023-01-01'])
	data.append([30])
	data.append([400000])
	data.append(['Annuity'])
	data.append([0])
	table = pa.table(data, names=colNames)
	print([table])

if __name__ == '__main__':
	main()