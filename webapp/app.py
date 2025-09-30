import flask, requests, json, os, httpx, asyncio, threading, storage_functionalities

app = flask.Flask(__name__, template_folder = "template_files", static_folder = "static_files")

SETUP_FUNCTION_URL = os.getenv("setup_function_url")
STARTER_FUNCTION_URL = os.getenv("starter_function_url")

@app.route('/', methods = ["GET", "POST"])
def index():
    return flask.render_template('index.html')

@app.route('/storage-results', methods = ["GET", "POST"])
def storage_results():
    structure = storage_functionalities.get_storage_structure()
    return flask.render_template('storage.html', structure = structure)

@app.route('/download/<marketplace>/<level>/<filename>', methods = ["GET", "POST"])
def download_file(marketplace, level, filename):
    content = storage_functionalities.get_blob_content(marketplace, level, filename)
    return flask.Response(
        content,
        mimetype="application/octet-stream",
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )

@app.route('/mongodb-results', methods = ["GET", "POST"])
def mongodb_results():
    marketplaces = storage_functionalities.get_marketplaces()
    return flask.render_template('mongo.html', marketplaces = marketplaces)

@app.route('/marketplace-results', methods = ["GET", "POST"])
def marketplace_results():
    marketplace_name = flask.request.form["marketplace-name"]

    results = storage_functionalities.get_marketplace_results(marketplace_name)
    marketplaces = storage_functionalities.get_marketplaces()
    return flask.render_template('mongo.html', results = results, marketplaces = marketplaces)

@app.route('/add-marketplace', methods = ["GET", "POST"])
def add_marketplace():
    marketplace_name = flask.request.form["new-marketplace-name"]
    cookies = flask.request.form["new-cookies"]
    has_cookies = flask.request.form["has-cookie-value"]
    
    storage_functionalities.add_marketplace(marketplace_name, has_cookies, cookies)
    return flask.redirect('/mongodb-results')

@app.route('/remove-marketplace', methods = ["GET", "POST"])
def remove_marketplace():
    marketplace_name = flask.request.form["marketplace-name"]
    
    storage_functionalities.remove_marketplace(marketplace_name)
    return flask.redirect('/mongodb-results')

@app.route('/add-cookie', methods = ["GET", "POST"])
def add_cookie():
    marketplace_name = flask.request.form["marketplace-name"]
    cookie_name = flask.request.form["cookie-name"]
    cookie_value = flask.request.form["cookie-value"]
    
    storage_functionalities.add_cookie_to_marketplace(marketplace_name, cookie_name, cookie_value)
    return flask.redirect('/mongodb-results')

@app.route('/remove-cookie', methods = ["GET", "POST"])
def remove_cookie():
    marketplace_name = flask.request.form["marketplace-name"]
    cookie_name = flask.request.form["cookie-name"]
    
    storage_functionalities.remove_cookie_from_marketplace(marketplace_name, cookie_name)
    return flask.redirect('/mongodb-results')

@app.route('/setup-crawl', methods = ["GET", "POST"])
def setup_crawl():
    crawler_depth = flask.request.form["crawler-depth"]
    max_links = flask.request.form["max-links"]
    concurrent_workers = flask.request.form["concurrent-workers"]
    start_url = flask.request.form["start-url"]
    marketplace = flask.request.form["marketplace"]
    
    
    payload = {
        "root_url": start_url,
        "max_workers": concurrent_workers,
        "max_depth": crawler_depth,
        "max-links": max_links,
        "marketplace": marketplace
    }

    headers = {
        "Content-Type": "application/json"
    }
    
    # Testing payload input
        # return flask.jsonify(payload)

    # Request to azure function app
    response = requests.post(SETUP_FUNCTION_URL, data = json.dumps(payload), headers = headers)
    if response.status_code != 200: return response.text, 500
    
    response_starter = requests.post(STARTER_FUNCTION_URL, data = json.dumps(payload), headers = headers)
    if response_starter.status_code == 200:
        return flask.redirect(flask.url_for("index", instance_id = response_starter.text))
    
    return response_starter.text, 500


async def fire_and_forget(function_url, payload):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(function_url, json=payload, timeout=0.1)
        except httpx.ReadTimeout:
            pass

if __name__ == "__main__":
    app.run(port = 8080, debug = True)