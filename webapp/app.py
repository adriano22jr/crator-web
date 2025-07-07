import flask, requests, app_config, json, os

app = flask.Flask(__name__, template_folder = "template_files", static_folder = "static_files")


FUNCTION_APP_HOSTNAME = os.getenv("functionapp_hostname")

@app.route('/', methods = ["GET", "POST"])
def index():
    return flask.render_template('index.html')


@app.route('/setup-crawl', methods = ["GET", "POST"])
def setup_crawl():
    crawler_depth = flask.request.form["crawler-depth"]
    max_links = flask.request.form["max-links"]
    max_exec_time = flask.request.form["max-execution-time"]
    concurrent_workers = flask.request.form["concurrent-workers"]
    start_url = flask.request.form["start-url"]
    
    
    # function_url = f"https://{FUNCTION_APP_HOSTNAME}/api/crawling_setup?code=gmv-M9j3bjoXCseX09yuAOV6TYk7l_jViQUeKTR_jUjHAzFuzd-PXQ%3D%3D"
    function_url = f"http://localhost:7071/api/crawling_setup"
    payload = {
        "root_url": start_url,
        "max_workers": concurrent_workers,
        "max_depth": crawler_depth,
        "max-links": max_links,
        "max-execution-time": max_exec_time
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(function_url, data = json.dumps(payload), headers = headers)
    return response.text

if __name__ == "__main__":
    app.run(port = 8080, debug = True)