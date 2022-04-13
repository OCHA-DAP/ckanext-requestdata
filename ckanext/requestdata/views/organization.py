from collections import Counter

from flask import Blueprint

import ckanext.hdx_org_group.helpers.org_meta_dao as org_meta_dao
import ckanext.hdx_org_group.helpers.organization_helper as helper
from ckan import model
from ckan.plugins import toolkit as tk
from ckan.views.group import _setup_template_variables as _setup_template_variables
from ckanext.requestdata import helpers

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

    context = {
        'model': model,
        'session': model.Session,
        'user': g.user
    }

    g.group_dict = _get_action('organization_show', {'id': id})
    group_type = 'organization'
    request_params = request.form.to_dict()

    filtered_maintainers = []
    reverse = True
    order = 'last_request_created_at'
    q_organization = ''
    current_order_name = 'Most Recent'

    for item in request_params:
        if item == 'filter_by_maintainers':
            for x in request_params[item]:
                params = x.split('|')
                org = params[0].split(':')[1]
                maintainers = params[1].split(':')[1].split(',')
                maintainers_ids = []
                if maintainers[0] != '*all*':
                    for i in maintainers:
                        try:
                            user = _get_action('user_show', {'id': i})
                            maintainers_ids.append(user['id'])
                        except NotFound:
                            pass
                    data = {
                        'org': org,
                        'maintainers': maintainers_ids
                    }

                    filtered_maintainers.append(data)
        elif item == 'order_by':
            params = request_params[item][0].split('|')
            q_organization = params[1].split(':')[1]
            order = params[0]
            if 'asc' in order:
                reverse = False
                order = 'title'
                current_order_name = 'Alphabetical (A-Z)'
            elif 'desc' in order:
                reverse = True
                order = 'title'
                current_order_name = 'Alphabetical (Z-A)'
            elif 'most_recent' in order:
                reverse = True
                order = 'last_request_created_at'
            elif 'shared' in order:
                current_order_name = 'Sharing Rate'
            elif 'requests' in order:
                current_order_name = 'Requests Rate'

            for x in requests:
                package = \
                    _get_action('package_show', {'id': x['package_id']})
                count = \
                    _get_action('requestdata_request_data_counters_get',
                                {'package_id': x['package_id']})
                x['title'] = package['title']
                x['shared'] = count.shared
                x['requests'] = count.requests
                data_dict = {'id': package['owner_org']}
                current_org = _get_action('organization_show', data_dict)
                x['name'] = current_org['name']

    maintainers = []
    for item in requests:
        package = _get_action('package_show', {'id': item['package_id']})
        package_maintainer_ids = package['maintainer'].split(',')
        item['title'] = package['title']
        package_maintainers = []

        for maint_id in package_maintainer_ids:
            try:
                user = _get_action('user_show', {'id': maint_id})
                username = user['name']
                name = user['fullname']

                if not name:
                    name = username

                payload = {
                    'id': maint_id,
                    'name': name,
                    'username': username,
                    'fullname': name}
                maintainers.append(payload)
                package_maintainers.append(payload)
            except NotFound:
                pass
        item['maintainers'] = package_maintainers

    copy_of_maintainers = maintainers
    maintainers = dict((item['id'], item) for item in maintainers).values()
    organ = _get_action('organization_show', {'id': id})

    # Count how many requests each maintainer has
    for main in maintainers:
        count = Counter(
            item for dct in copy_of_maintainers for item in dct.items())
        main['count'] = count[('id', main['id'])]

    # Sort maintainers by number of requests
    maintainers = sorted(
        maintainers, key=lambda k: k['count'], reverse=True)

    for i, r in enumerate(requests[:]):
        maintainer_found = False

        package = _get_action('package_show', {'id': r['package_id']})
        package_maintainer_ids = package['maintainer'].split(',')
        is_hdx = helpers.is_hdx_portal()

        if is_hdx:
            # Quick fix for hdx portal
            maintainer_ids = []
            for maintainer_name in package_maintainer_ids:
                try:
                    main_ids = \
                        _get_action('user_show', {'id': maintainer_name})
                    maintainer_ids.append(main_ids['id'])
                except NotFound:
                    pass
        data_dict = {'id': package['owner_org']}
        organ = _get_action('organization_show', data_dict)

        # Check if current request is part of a filtered maintainer
        for x in filtered_maintainers:
            if x['org'] == organ['name']:
                for maint in x['maintainers']:
                    if is_hdx:
                        if maint in maintainer_ids:
                            maintainer_found = True
                    else:
                        if maint in package_maintainer_ids:
                            maintainer_found = True

                if not maintainer_found:
                    requests.remove(r)

    requests_new = []
    requests_open = []
    requests_archive = []

    for item in requests:
        if item['state'] == 'new':
            requests_new.append(item)
        elif item['state'] == 'open':
            requests_open.append(item)
        elif item['state'] == 'archive':
            requests_archive.append(item)

    grouped_requests_archive = helpers.group_archived_requests_by_dataset(requests_archive)

    if order == 'last_request_created_at':
        for dataset in grouped_requests_archive:
            created_at = dataset.get('requests_archived')[0].get('created_at')
            data = {
                'last_request_created_at': created_at
            }
            dataset.update(data)

    if organ['name'] == q_organization:
        grouped_requests_archive = sorted(grouped_requests_archive,
                                          key=lambda x: x[order],
                                          reverse=reverse)

    counters = _get_action('requestdata_request_data_counters_get_by_org', {'org_id': organ['id']})

    extra_vars = {
        'requests_new': requests_new,
        'requests_open': requests_open,
        'requests_archive': grouped_requests_archive,
        'maintainers': maintainers,
        'org_name': organ['name'],
        'current_order_name': current_order_name,
        'org_meta': org_meta,
        'counters': counters
    }

    _setup_template_variables(context, {'id': id}, group_type=group_type)

    helper.org_add_last_updated_field([org_meta.org_dict])
    if org_meta.is_custom:
        return render('requestdata/custom_organization_requested_data.html', extra_vars)
    else:
        return render('requestdata/organization_requested_data.html', extra_vars)


requestdata_organization_requests.add_url_rule(u'/<id>', view_func=requested_data, methods=[u'GET'])
