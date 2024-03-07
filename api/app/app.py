from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)

@app.route('/')
def hello_ecommerce():
    return '<h1>Aplicação e-commerce!</h1>'

#closed_deals 
@app.route('/api/closed_deals/',methods=['GET'])
def closed_deals():
    table_deals = pd.read_csv('olist_closed_deals_dataset.csv')
    
    # pagination parameters
    page = int(request.args.get('page', 1))
    items_per_page = int(request.args.get('items_per_page', 10))

    # indice of page
    start_index = (page - 1) * items_per_page
    end_index = start_index + items_per_page
    
    filter_data = table_deals

    #filter mql, seller and business type
    mql = request.args.getlist('mql_id')
    seller = request.args.getlist('seller_id')
    business_segment = request.args.getlist('business_segment')
    
    if mql:
        filter_data = filter_data[filter_data['mql_id'].isin(mql)]
    if seller:
        filter_data = filter_data[filter_data['seller_id'].isin(seller)]
    if business_segment:
        filter_data = filter_data[filter_data['business_segment'].isin(business_segment)]
        
    
    # current page filter
    page_deals = filter_data.iloc[start_index:end_index]
    page_deals = page_deals.to_json(orient='records')
    
    # table_deals = table_deals.to_json(orient="records")
    return jsonify(page_deals)


# qualified_leads 
@app.route('/api/qualified_leads/',methods=['GET'])
def qualified_leads():
    table_mql = pd.read_csv('olist_marketing_qualified_leads_dataset.csv')
    
    # pagination parameters
    page = int(request.args.get('page', 1))
    items_per_page = int(request.args.get('items_per_page', 10))

    # indice of page
    start_index = (page - 1) * items_per_page
    end_index = start_index + items_per_page
    
    filter_data = table_mql

    #filter mql, seller and business type
    mql = request.args.getlist('mql_id')
    first_contact_date = request.args.getlist('first_contact_date')
    origin = request.args.getlist('origin')
    
    if mql:
        filter_data = filter_data[filter_data['mql_id'].isin(mql)]
    if first_contact_date:
        filter_data = filter_data[filter_data['first_contact_date'].isin(first_contact_date)]
    if origin:
        filter_data = filter_data[filter_data['origin'].isin(origin)]
        
    
    # current page filter
    page_deals = filter_data.iloc[start_index:end_index]
    page_deals = page_deals.to_json(orient='records')
    
    # table_deals = table_deals.to_json(orient="records")
    return jsonify(page_deals)
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)