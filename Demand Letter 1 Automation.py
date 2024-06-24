import os
import pandas as pd

def delinquencyBucket(balance, payment_amount):
    if (balance / payment_amount > 0 and balance / payment_amount < 5):
        return "1:1-4 weeks"
    elif (balance / payment_amount >= 5 and balance / payment_amount < 9):
        return "2:5-8 weeks"
    elif (balance / payment_amount >= 9 and balance / payment_amount < 13):
        return "3:9-12 weeks"
    elif (balance / payment_amount >= 13 and balance / payment_amount < 17):
        return "4:13-16 weeks"
    elif (balance / payment_amount >= 17 and balance / payment_amount < 21):
        return "5:17-20 weeks"
    elif (balance / payment_amount >= 21 and balance / payment_amount < 25):
        return "6:21-24 weeks"
    elif (balance / payment_amount >= 25):
        return "7:25+ weeks"
    else:
        return "0:current"

os.chdir('')

balance_limit = 10
customer_balance_limit = 250
ineligble_account_status = ['Discharged', 'Fraud', 'Furniture', 'Killed', 'No Recourse - BNK',
                            'No Recourse - Deceased', 'Under Investigation', 'Unknown', 'unknown',
                            'Special']
cut_off_date = pd.to_datetime("2024-06-01")
days_since_last_payment_limit = 60

columns_to_read =[
    "Account Number", "agreement_id", "Product Type", "Store, Affiliate or Brand Name", "First name", "Last name", "Address", "City", "State",
    "Zip Code", "SSN", "Date of Birth", "Home Phone Number", "Cell Phone Number", "Email Address", "Account Status",
    "Account Open Date", "Current Balance", "Lease Balance", "Fees Balance", "Last Payment Date", "Last Payment Amount",
    "Total lifetime payments", "Total Number of Lifetime Leases", "Lease Source", "Multiple / Markup", "Name of Servicer",
    "Creditor's Name at Charge-Off", "Account Owner's Name at Charge-Off", "Creditor's Address at Charge-Off",
    "Collateral Type", "Items Rented", "Collateral value", "Chapter", "Filing Date", "Case Number", "Bankruptcy Court",
    "weekly_payment_amount", "Maturity Date (contractual maturity)", "sign_date", "reference_id"
]
dtype_mapping = {
    "Account Number": str,
    "Product Type": str,
    "Store, Affiliate or Brand Name": str,
    "First name": str,
    "Last name": str,
    "City": str,
    "State": str,
    "Zip Code": str,
    "SSN": float,
    "Home Phone Number": str,
    "Cell Phone Number": str,
    "Email Address": str,
    "Account Status": str,
    "Current Balance": float,
    "Lease Balance": float,
    "Fees Balance": float,
    "Last Payment Amount": float,
    "Total lifetime payments": float,
    "Total Number of Lifetime Leases": float,
    "Lease Source": str,
    "Multiple / Markup": float,
    "Name of Servicer": str,
    "Creditor's Name at Charge-Off": str,
    "Account Owner's Name at Charge-Off": str,
    "Creditor's Address at Charge-Off": str,
    "Collateral Type": str,
    "Items Rented": str,
    "Collateral value": float,
    "Chapter": str,
    "Case Number": str,
    "Bankruptcy Court": str,
    "weekly_payment_amount": float,
    "agreement_id": str,
    "reference_id": str,
    "Address": str
}
dates_to_be_parsed = [
    "Date of Birth",
    "Account Open Date",
    "Last Payment Date",
    "Filing Date",
    "Maturity Date (contractual maturity)",
    "sign_date"
]

populationData1 = pd.read_csv("population_06-06-2024_1.csv", usecols=columns_to_read, dtype=dtype_mapping, parse_dates=dates_to_be_parsed)[columns_to_read]
populationData2 = pd.read_csv("population_06-06-2024_2.csv", usecols=columns_to_read, dtype=dtype_mapping, parse_dates=dates_to_be_parsed)[columns_to_read]
finalPopulation = pd.concat([populationData1, populationData2])

previousSold = pd.read_csv('Debt Sale Summary - Master.csv', usecols=['Account_Number'], dtype={"Account_Number": str})
ineligibleForSale = pd.read_csv('Debt Sale Summary - Ineligible for Sale.csv', usecols=['Account Number'], dtype={"Account Number": str})
promiseToPay = pd.read_csv('PTP Data 06-06-2024.csv', usecols=['legacy_user_id'], dtype={"legacy_user_id": str})
dnc = pd.read_csv('DNC Master List 06-06-24.csv', usecols=['PHONE'], dtype={"PHONE": str})

finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(previousSold['Account_Number'])] # Previously Sold Exclusion
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(ineligibleForSale['Account Number'])] # Ineligible for Sale Exclusion
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(promiseToPay['legacy_user_id'])] # PTP Exclusion

finalPopulation = finalPopulation[~finalPopulation['Home Phone Number'].isin(dnc['PHONE'])] # Home DNC Exclusion
finalPopulation = finalPopulation[~finalPopulation['Cell Phone Number'].isin(dnc['PHONE'])] # Cell DNC Exclusion

finalPopulation = finalPopulation[finalPopulation['Current Balance'] >= balance_limit] # Lease Level Balance Exclusion
finalPopulation = finalPopulation[~finalPopulation['Account Status'].isin(['Canceled', 'Returned', 'Settled'])] # Lease Level Status Exclusion

# Customer Level Balance Exclusion
customer_balances = finalPopulation[['Account Number', 'Current Balance']]
total_balances = customer_balances.groupby('Account Number')['Current Balance'].sum()
customer_balances_to_exclude = pd.Series(total_balances[total_balances < customer_balance_limit].index)
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(customer_balances_to_exclude)]

# Customer Level Status Exclusion
customer_statuses = finalPopulation[['Account Number', 'Account Status']]
customer_statuses_exclusion = customer_statuses[customer_statuses['Account Status'].isin(ineligble_account_status)]['Account Number'].unique()
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(customer_statuses_exclusion)]

# Age Exclusion
customer_dob = finalPopulation[['Account Number', 'Date of Birth']]
customer_dob['Age'] = (cut_off_date - customer_dob['Date of Birth']).dt.days / 365
customer_age_exclusion = customer_dob[(customer_dob['Age'] > 80) | (customer_dob['Age'] < 18)]['Account Number']
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(customer_age_exclusion)]

# Last Positive Payment Exclusion
customer_last_payment_date = finalPopulation[['Account Number', 'Last Payment Date']]
customer_last_payment_date['Days Since Payment'] = (cut_off_date - customer_last_payment_date['Last Payment Date']).dt.days
customer_last_payment_date_exclusion = customer_last_payment_date[customer_last_payment_date['Days Since Payment'] <= days_since_last_payment_limit]['Account Number']
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(customer_last_payment_date_exclusion)]

# Contractual Maturity Post Cutoff Date
customer_post_maturity = finalPopulation[['Account Number', 'Maturity Date (contractual maturity)']]
customer_post_maturity_exclusion = customer_post_maturity[(customer_post_maturity['Maturity Date (contractual maturity)'] > cut_off_date)]['Account Number']
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(customer_post_maturity_exclusion)]

# Secondary Maturity Exclusion - Days Mature
customer_maturity_days = finalPopulation[['Account Number', 'sign_date']]
customer_maturity_days_exclusion = customer_maturity_days[((cut_off_date - customer_maturity_days['sign_date']).dt.days + 21) < 365]['Account Number']
finalPopulation = finalPopulation[~finalPopulation['Account Number'].isin(customer_maturity_days_exclusion)]

# Remove Test Accounts
finalPopulation = finalPopulation[~finalPopulation['Address'].str.contains("901 Yamato Rd #260", na=False)]
finalPopulation = finalPopulation[~finalPopulation['First name'].str.contains("Ivr", na=False)]
finalPopulation = finalPopulation[~finalPopulation['Email Address'].str.contains("flexshopper", na=False)]

# Add in Delinquency Bucket
finalPopulation['Delinquency Bucket'] = finalPopulation.apply(lambda x: delinquencyBucket(x['Current Balance'], x['weekly_payment_amount']), axis=1)
finalPopulation = finalPopulation[~finalPopulation['Delinquency Bucket'].str.contains('0:current|1:1-4 weeks|2:5-8 weeks', case=False, regex=True)]

# Final Population
finalPopulation.to_csv("Test.csv", index=False)
