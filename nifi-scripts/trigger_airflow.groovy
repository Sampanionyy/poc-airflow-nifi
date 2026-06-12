/**
 * Script NiFi (ExecuteScript - Groovy)
 * Déclenche le DAG Airflow "demo_pipeline" via l'API REST
 * en transmettant le nom du fichier détecté par NiFi.
 */

import java.util.Base64

def flowFile = session.get()
if (!flowFile) return

def filename = flowFile.getAttribute('filename') ?: 'clients.csv'
def auth     = Base64.getEncoder().encodeToString("admin:admin".getBytes("UTF-8"))
def body     = """{"conf": {"source": "nifi", "file": "${filename}"}}"""

log.info("NiFi → Airflow : déclenchement du DAG pour le fichier '${filename}'")

try {
    def url  = new URL("http://airflow:8080/api/v1/dags/demo_pipeline/dagRuns")
    def conn = (java.net.HttpURLConnection) url.openConnection()
    conn.requestMethod = 'POST'
    conn.doOutput     = true
    conn.connectTimeout = 10_000
    conn.readTimeout    = 15_000
    conn.setRequestProperty('Content-Type',  'application/json')
    conn.setRequestProperty('Authorization', "Basic ${auth}")

    conn.outputStream.withWriter('UTF-8') { it.write(body) }

    def code = conn.responseCode
    if (code >= 200 && code < 300) {
        log.info("DAG déclenché avec succès (HTTP ${code})")
        session.transfer(flowFile, REL_SUCCESS)
    } else {
        def err = conn.errorStream?.text ?: '(pas de corps de réponse)'
        log.error("Échec déclenchement DAG : HTTP ${code} — ${err}")
        session.transfer(flowFile, REL_FAILURE)
    }
    conn.disconnect()

} catch (Exception e) {
    log.error("Exception lors du déclenchement Airflow : ${e.getMessage()}")
    session.transfer(flowFile, REL_FAILURE)
}
