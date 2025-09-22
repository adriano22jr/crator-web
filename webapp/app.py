import flask, requests, app_config, json, os

app = flask.Flask(__name__, template_folder = "template_files", static_folder = "static_files")


SETUP_FUNCTION_URL = os.getenv("setup_function_url")

@app.route('/', methods = ["GET", "POST"])
def index():
    return flask.render_template('index.html')


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
    return response.text

if __name__ == "__main__":
    app.run(port = 8080, debug = True)