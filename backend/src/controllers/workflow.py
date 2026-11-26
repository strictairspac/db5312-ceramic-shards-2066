from starlette.responses import Response

from src.services.workflow import fetch_evidence
from src.utils.encoding import canonical


def download(identity):
    data = fetch_evidence(identity)
    return Response(canonical(data), media_type='application/json', headers={
        'Content-Disposition': f'attachment; filename="evidence-{identity}.json"'})
