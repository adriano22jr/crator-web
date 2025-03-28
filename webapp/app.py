import flask, requests, app_config

app = flask.Flask(__name__, template_folder = "template_files", static_folder = "static_files")

@app.route('/', methods = ["GET", "POST"])
def index():
    return flask.render_template('index.html')


# TODO: change this to an azure functions-call, and verify the correctness of the azure-function step for the crawling setup.
@app.route('/setup-crawl', methods = ["GET", "POST"])
def setup_crawl():
    crawler_depth = flask.request.form["crawler-depth"]
    max_links = flask.request.form["max-links"]
    max_exec_time = flask.request.form["max-execution-time"]
    concurrent_workers = flask.request.form["concurrent-workers"]
    start_url = flask.request.form["start-url"]
    
    text = f"{crawler_depth}, {max_links}, {max_exec_time}, {concurrent_workers}, {start_url}"
    response = flask.make_response(text, 200)
    response.mimetype = "text/plain"
    return response

if __name__ == "__main__":
    app.run(port = 8080, debug = True)