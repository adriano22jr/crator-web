import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()

@app.route(route = "test", auth_level = func.AuthLevel.ANONYMOUS)
def test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    number = req.params.get('number')
    if not number:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            number = req_body.get('number')

    if number:
        number += 100
        return func.HttpResponse(f"{number - 100} + 100 equals = {number}")
    else:
        return func.HttpResponse(
             "No number provided!",
             status_code = 500
        )