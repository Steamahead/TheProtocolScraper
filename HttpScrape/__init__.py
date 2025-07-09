import logging
import azure.functions as func
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function started - minimal version')
    
    return func.HttpResponse(
        body=json.dumps({'status': 'success', 'message': 'Function is working'}),
        status_code=200,
        mimetype='application/json'
    )
