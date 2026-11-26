from fastapi import APIRouter, Depends, Request, Response

from src.config.settings import cache, engine
from sqlalchemy import text

from src.controllers.workflow import download
from src.models.tables import audit, cases, evidence, runs
from src.schemas.workflow import CaseForm, LoginForm, ReviewForm
from src.services import domain, workflow
from src.services.security import (current_user, current_user_optional, login,
                                   operator, reviewer)

router = APIRouter()

_CASE_QUERY_KEYS = ('case_id', 'caseId', 'case', 'archive_id', 'archiveId', 'archive',
                    'business_case_id', 'businessCaseId', 'id')


def _selected_case_id(request: Request, explicit=None):
    """Resolve an archive selector from many plausible query parameter names."""
    if explicit is not None:
        return explicit
    query = request.query_params
    for key in _CASE_QUERY_KEYS:
        value = query.get(key)
        if value and str(value).strip().lstrip('-').isdigit():
            return int(value)
    snapshot = query.get('snapshot_id') or query.get('snapshotId') or query.get('snapshot')
    if snapshot:
        ids = workflow.migration.resolve_case_ids([snapshot])
        if ids:
            return ids[0]
    return None


@router.get('/health')
def health():
    with engine.connect() as conn:
        conn.execute(text('SELECT 1')).scalar_one()
    cache.ping()
    return {'ok': True}


@router.post('/login')
def sign_in(form: LoginForm):
    return login(form.username, form.password)


@router.get('/spec')
def spec(_=Depends(current_user)):
    return domain.SPEC


@router.get('/cases')
def list_cases(_=Depends(current_user_optional)):
    return workflow.recent(cases)


@router.post('/cases', status_code=201)
def create_case(form: CaseForm, actor=Depends(operator)):
    return workflow.make_case(form.title, form.payload, actor)


@router.get('/cases/{case_id}')
def get_case(case_id: int, _=Depends(current_user_optional)):
    return workflow.case_detail(case_id)


@router.get('/cases/{case_id}/runs')
def list_case_runs(case_id: int, response: Response, _=Depends(current_user_optional)):
    rows = workflow.runs_for_case(case_id)
    response.headers['X-Database'] = rows.database
    response.headers['X-Active-Database'] = rows.active_database
    return rows


@router.get('/cases/{case_id}/evidence')
def list_case_evidence(case_id: int, response: Response, _=Depends(current_user_optional)):
    rows = workflow.evidence_for_case(case_id)
    response.headers['X-Database'] = rows.database
    response.headers['X-Active-Database'] = rows.active_database
    return rows


@router.get('/runs')
def list_runs(request: Request, response: Response, case_id: int | None = None,
              _=Depends(current_user_optional)):
    selected = _selected_case_id(request, case_id)
    if selected is not None:
        rows = workflow.runs_for_case(selected)
        response.headers['X-Database'] = rows.database
        return rows
    return workflow.recent(runs)


@router.post('/cases/{case_id}/runs', status_code=202)
def start_run(case_id: int, actor=Depends(operator)):
    return workflow.enqueue(case_id, actor)


@router.get('/evidence')
def list_evidence(request: Request, response: Response, case_id: int | None = None,
                  run_id: int | None = None, _=Depends(current_user_optional)):
    selected = _selected_case_id(request, case_id)
    if selected is not None:
        rows = workflow.evidence_for_case(selected)
        response.headers['X-Database'] = rows.database
        return rows
    if run_id is not None:
        return workflow.evidence_for_run(run_id)
    return workflow.recent(evidence)


@router.get('/evidence/{identity}/download')
def export_evidence(identity: int, _=Depends(current_user_optional)):
    return download(identity)


@router.post('/evidence/{identity}/review')
def review_evidence(identity: int, form: ReviewForm, actor=Depends(reviewer)):
    return workflow.review(identity, form.note, actor)


@router.get('/audit')
def list_audit(_=Depends(current_user)):
    return workflow.recent(audit)
