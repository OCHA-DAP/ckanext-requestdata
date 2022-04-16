import logging
from flask import Blueprint

import ckanext.hdx_org_group.helpers.org_meta_dao as org_meta_dao
import ckanext.hdx_org_group.helpers.organization_helper as helper
from ckan import model
from ckan.plugins import toolkit as tk
from ckanext.requestdata import helpers
from ckanext.requestdata.view_helper import find_archived_sorting_params, sort_archived, group_requests_by_state, \
    build_id_to_user_map, populate_requests_with_package_title_and_maintainer

log = logging.getLogger(__name__)

NotFound = tk.ObjectNotFound
NotAuthorized = tk.NotAuthorized
ValidationError = tk.ValidationError

abort = tk.abort
g = tk.g
_ = tk._
request = tk.request
h = tk.h
_check_access = tk.check_access
__get_action = tk.get_action
config = tk.config
render = tk.render

GROUP_TYPES = ['organization']

requestdata_organization_requests = Blueprint(u'requestdata_organization_requests', __name__,
                                              url_prefix=u'/organization/requested_data')


def _get_context():
    return {
        'model': model,
        'session': model.Session,
        'user': g.user or g.author,
        'auth_user_obj': g.userobj
    }


def _get_action(action, data_dict):
    return __get_action(action)(_get_context(), data_dict)


def requested_data(id):
    '''Handles creating template for 'Requested Data' page in the
    organization's dashboard.
    :param id: The organization's id.
    :type id: string
    :returns: template
    '''

    try:
        requests = _get_action('requestdata_request_list_for_organization', {'org_id': id})
    except NotAuthorized:
        return abort(403, _('Not authorized to see this page.'))

    org_meta = org_meta_dao.OrgMetaDao(id, g.user or g.author, g.userobj)
    org_meta.fetch_all()

    request_params = request.args.to_dict() or request.form.to_dict()

    id_to_user_map = build_id_to_user_map(requests)

    populate_requests_with_package_title_and_maintainer(requests, id_to_user_map)

    # Sort maintainers by number of requests
    maintainers = sorted(id_to_user_map.values(), key=lambda k: k['count'], reverse=True)

    _filter_by_maintainer(requests, request_params.get('filter_by_maintainers'), id_to_user_map)

    requests_archive, requests_new, requests_open = group_requests_by_state(requests)

    grouped_requests_archive = helpers.group_archived_requests_by_dataset(requests_archive)
    order, reverse, current_order_name = find_archived_sorting_params(request_params.get('order_by'))
    grouped_requests_archive = sort_archived(grouped_requests_archive, order, reverse)

    counters = _get_action('requestdata_request_data_counters_get_by_org', {'org_id': org_meta.org_dict['id']})

    extra_vars = {
        'requests_new': requests_new,
        'requests_open': requests_open,
        'requests_archive': grouped_requests_archive,
        'maintainers': maintainers,
        'org_name': org_meta.org_dict['name'],
        'current_order_name': current_order_name,
        'org_meta': org_meta,
        'counters': counters
    }

    helper.org_add_last_updated_field([org_meta.org_dict])
    if org_meta.is_custom:
        return render('requestdata/custom_organization_requested_data.html', extra_vars)
    else:
        return render('requestdata/organization_requested_data.html', extra_vars)


def _filter_by_maintainer(requests, request_param_value, id_to_user_map):
    filtered_maintainers = _build_filtered_maintainers_set(request_param_value)
    for current_request in requests[:]:
        package = current_request['package_dict']
        package_maintainer_id = package['maintainer']
        package_maintainer_name = id_to_user_map[package_maintainer_id]['username']

        if filtered_maintainers and package_maintainer_name not in filtered_maintainers:
            requests.remove(current_request)


def _build_filtered_maintainers_set(request_param_value):
    if request_param_value:
        params = request_param_value.split('|')
        # org = params[0].split(':')[1]
        maintainers = params[1].split(':')[1].split(',')
        if maintainers[0] != '*all*':
            return set(maintainers)
    return None


requestdata_organization_requests.add_url_rule(u'/<id>', view_func=requested_data, methods=[u'GET'])
