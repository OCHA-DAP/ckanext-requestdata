try:
    # CKAN 2.7 and later
    from ckan.common import config
except ImportError:
    # CKAN 2.6 and earlier
    from pylons import config
from ckan.plugins import toolkit
from ckan import model
from ckan.common import c, _
import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.lib.maintain as maintain
import ckanext.requestdata.helpers as requestdata_helper
import ckan.logic as logic
import unicodecsv as csv
import json
from cStringIO import StringIO
from collections import Counter
from sqlalchemy.sql.expression import or_
from ckanext.requestdata import helpers

from ckan.common import response, request

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized

# redirect = base.redirect
abort = base.abort


BaseController = toolkit.BaseController


def _get_context():
    return {
        'model': model,
        'session': model.Session,
        'user': c.user or c.author,
        'auth_user_obj': c.userobj
    }


def _get_action(action, data_dict):
    return toolkit.get_action(action)(_get_context(), data_dict)


class AdminController(BaseController):
    ctrl = 'ckanext.requestdata.controllers.admin:AdminController'

    def __before__(self, action, **params):
        super(AdminController, self).__before__(action, **params)
        context = {'model': model,
                   'user': c.user, 'auth_user_obj': c.userobj}
        try:
            logic.check_access('hdx_request_data_admin_list', context, {})
        except logic.NotAuthorized:
            base.abort(403, _('Need to be request data administrator to administer'))
        c.revision_change_state_allowed = True

    def email(self):
        '''
            Handles creating the email template in admin dashboard.

            :returns template
        '''
        data = request.POST
        if 'save' in data:
            try:
                data_dict = dict(request.POST)
                del data_dict['save']
                data = _get_action('config_option_update', data_dict)
                h.flash_success(_('Successfully updated.'))
            except logic.ValidationError, e:
                errors = e.error_dict
                error_summary = e.error_summary
                vars = {'data': data, 'errors': errors,
                        'error_summary': error_summary}
                return base.render('admin/email.html', extra_vars=vars)

            h.redirect_to(controller=self.ctrl, action='email')

        schema = logic.schema.update_configuration_schema()
        data = {}
        for key in schema:
            data[key] = config.get(key)

        vars = {'data': data, 'errors': {}}
        return toolkit.render('admin/email.html', extra_vars=vars)

    def requests_data(self):
        requests = []
        try:
            requests = _get_action('requestdata_request_list_for_sysadmin', {})
        except NotAuthorized:
            abort(403, _('Not authorized to see this page.'))
        package_ids = {r.get('package_id') for r in requests}

        package_ids_to_requests = {}
        for r in requests:
            pkg_id = r.get('package_id')
            req_list = package_ids_to_requests.get(pkg_id)
            if not req_list:
                req_list = []
                package_ids_to_requests[pkg_id] = req_list

            req_list.append(r)

        search_result = self.__find_packages(package_ids)
        maintainer_ids = {pkg_dict.get('maintainer')
                          for pkg_dict in search_result.get('results', []) if pkg_dict.get('maintainer')}
        maintainers_dict = self.__build_maintainers_dict(maintainer_ids)
        
        orgs_map = self.__build_organizations_dict(search_result.get('results'), package_ids_to_requests,
                                               maintainers_dict)
        filtered_orgs = self.__find_filtered_orgs()
        filtered_orgs_map = {k:v for k,v in orgs_map.items() if k in filtered_orgs} if filtered_orgs else orgs_map
        orgs = sorted(filtered_orgs_map.values(), key=lambda o: o['title'])

        total_requests_counters = \
            _get_action('requestdata_request_data_counters_get_all', {})
        extra_vars = {
            'organizations': orgs,
            'organizations_for_filters': sorted(((o['id'], o) for o in orgs_map.values()),
                                                key=lambda (org_id, o): o['requests'], reverse=True),
            'total_requests_counters': total_requests_counters
        }

        ret = toolkit.render('admin/all_requests_data.html', extra_vars)

        return ret

    @staticmethod
    def __find_packages(package_ids):
        id_filter = ' OR '.join(('"{}"'.format(id) for id in package_ids))
        query_string = 'id:({})'.format(id_filter)
        basic_query_params = {
            'start': 0,
            'rows': 2000,
            'q': ''
        }
        query_params = {'fq': query_string}
        query_params.update(basic_query_params)
        search_result = logic.get_action('package_search')({}, query_params)
        return search_result

    @staticmethod
    def __find_filtered_orgs():
        req_param = request.params.get('filter_by_organizations')
        if req_param:
            filtered_organizations = req_param.split(',')
            if filtered_organizations:
                return set(filtered_organizations)
        return set()

    @staticmethod
    def __build_maintainers_dict(maintainer_ids):
        maintainers_dict = {}
        query = model.Session.query(model.User)
        query = query.filter(or_(model.User.name.in_(maintainer_ids),
                                 model.User.id.in_(maintainer_ids),
                                 model.User.email.in_(maintainer_ids)))

        users = query.all()
        if users:
            maintainers_dict = {
                u.id: {
                    'id': u.id,
                    'name': u.fullname,
                    'username': u.name,
                    'fullname': u.fullname
                } for u in users
            }

        return maintainers_dict

    @staticmethod
    def __build_organizations_dict(package_list, package_ids_to_requests, maintainers_dict):
        orgs_map = {}
        for pkg_dict in package_list:
            org_dict = pkg_dict.get('organization')
            pkg_maintainer = maintainers_dict.get(pkg_dict.get('maintainer', ''))
            pkg_maintainers = [pkg_maintainer] if pkg_maintainer else []
            requests = package_ids_to_requests[pkg_dict['id']]
            for r in requests:
                r['title'] = pkg_dict.get('title')
                r['name'] = org_dict.get('name')
                r['maintainers'] = pkg_maintainers
            new_org_dict = orgs_map.get(org_dict['name'])

            archived_requests = [r for r in requests if r.get('state') == 'archive']
            grouped_archived_requests = {
                'package_id': pkg_dict['id'],
                'title': pkg_dict.get('title'),
                'maintainers': pkg_maintainers,
                'requests_archived': archived_requests,
                'requests': len(archived_requests),
                'shared': None,
            }

            if not new_org_dict:
                # counters = _get_action('requestdata_request_data_counters_get_by_org',
                #                 {'org_id': org_dict['id']})
                new_org_dict = {
                    'title': org_dict.get('title'),
                    'name': org_dict['name'],
                    'id': org_dict['id'],
                    'requests_new': [r for r in requests if r.get('state') == 'new'],
                    'requests_open': [r for r in requests if r.get('state') == 'open'],
                    'requests_archive': [grouped_archived_requests] if archived_requests else [],
                    'maintainers': [],
                    'counters': {},
                    'packages': [pkg_dict]
                }
                orgs_map[org_dict['name']] = new_org_dict
            else:
                new_org_dict['packages'].append(pkg_dict)
                new_org_dict['requests_archive'].append(grouped_archived_requests)
                for r in requests:
                    if r.get('state') == 'new':
                        new_org_dict['requests_new'].append(r)
                    elif r.get('state') == 'open':
                        new_org_dict['requests_open'].append(r)
                    # elif r.get('state') == 'archive':
                    #     new_org_dict['requests_archive'].append(r)

        for o in orgs_map.values():
            num_of_archived = sum((p['requests'] for p in o['requests_archive']))
            o['requests'] = num_of_archived + len(o['requests_open']) + len(o['requests_new'])

        return orgs_map

    @maintain.deprecated('replaced by new requests_data() that performs faster')
    def old_requests_data(self):
        '''
            DEPRECATED
            Handles creating template for 'Requested Data' page in the
            admin dashboard.

            :returns: template

        '''
        try:
            requests = _get_action('requestdata_request_list_for_sysadmin', {})
        except NotAuthorized:
            abort(403, _('Not authorized to see this page.'))
        organizations = []
        tmp_orgs = []
        filtered_maintainers = []
        filtered_organizations = []
        organizations_for_filters = {}
        reverse = True
        q_organizations = []
        request_params = request.params.dict_of_lists()
        order = 'last_request_created_at'

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
            elif item == 'filter_by_organizations':
                filtered_organizations = request_params[item][0].split(',')
            elif item == 'order_by':
                for x in request_params[item]:
                    params = x.split('|')
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
                        current_order_name = 'Most Recent'
                    elif 'shared' in order:
                        current_order_name = 'Sharing Rate'
                    elif 'requests' in order:
                        current_order_name = 'Requests Rate'

                    data = {
                        'org': q_organization,
                        'order': order,
                        'reverse': reverse,
                        'current_order_name': current_order_name
                    }

                    q_organizations.append(data)

                for x in requests:
                    package =\
                        _get_action('package_show', {'id': x['package_id']})
                    count = \
                        _get_action('requestdata_request_data_counters_get',
                                    {'package_id': x['package_id']})
                    if count:
                        x['shared'] = count.shared
                        x['requests'] = count.requests
                    x['title'] = package['title']
                    data_dict = {'id': package['owner_org']}
                    current_org = _get_action('organization_show', data_dict)
                    x['name'] = current_org['name']

        # Group requests by organization
        for item in requests[:50]:
            try:
                package = \
                    _get_action('package_show', {'id': item['package_id']})
                package_maintainer_ids = package['maintainer'].split(',')
                data_dict = {'id': package['owner_org']}
                org = _get_action('organization_show', data_dict)
                item['title'] = package['title']
            except NotFound, e:
                # package was not found, possibly deleted
                continue

            if org['id'] in organizations_for_filters:
                organizations_for_filters[org['id']]['requests'] += 1
            else:
                organizations_for_filters[org['id']] = {
                    'name': org['name'],
                    'title': org['title'],
                    'requests': 1
                }

            if len(filtered_organizations) > 0\
                    and org['name'] not in filtered_organizations:
                continue
            maintainers = []
            name = ''
            username = ''
            for id in package_maintainer_ids:
                try:
                    user = _get_action('user_show', {'id': id})
                    username = user['name']
                    name = user['fullname']
                    payload = {
                        'id': id,
                        'name': name,
                        'username': username,
                        'fullname': name
                    }
                    maintainers.append(payload)

                    if not name:
                        name = username
                except NotFound:
                    pass
            item['maintainers'] = maintainers
            counters = \
                _get_action('requestdata_request_data_counters_get_by_org',
                            {'org_id': org['id']})

            if org['id'] not in tmp_orgs:
                data = {
                    'title': org['title'],
                    'name': org['name'],
                    'id': org['id'],
                    'requests_new': [],
                    'requests_open': [],
                    'requests_archive': [],
                    'maintainers': [],
                    'counters': counters
                }

                if item['state'] == 'new':
                    data['requests_new'].append(item)
                elif item['state'] == 'open':
                    data['requests_open'].append(item)
                elif item['state'] == 'archive':
                    data['requests_archive'].append(item)

                payload = {'id': id, 'name': name, 'username': username}
                data['maintainers'].append(payload)

                organizations.append(data)
            else:
                current_org = \
                    next(item for item in organizations
                         if item['id'] == org['id'])

                payload = {'id': id, 'name': name, 'username': username}
                current_org['maintainers'].append(payload)

                if item['state'] == 'new':
                    current_org['requests_new'].append(item)
                elif item['state'] == 'open':
                    current_org['requests_open'].append(item)
                elif item['state'] == 'archive':
                    current_org['requests_archive'].append(item)

            tmp_orgs.append(org['id'])

        for org in organizations:
            copy_of_maintainers = org['maintainers']
            org['maintainers'] = \
                dict((item['id'], item)
                     for item in org['maintainers']).values()

            # Count how many requests each maintainer has
            for main in org['maintainers']:
                c = Counter(item for dct in copy_of_maintainers
                            for item in dct.items())
                main['count'] = c[('id', main['id'])]

            # Sort maintainers by number of requests
            org['maintainers'] = \
                sorted(org['maintainers'],
                       key=lambda k: k['count'],
                       reverse=True)

            total_organizations = \
                org['requests_new'] + \
                org['requests_open'] +\
                org['requests_archive']

            for i, r in enumerate(total_organizations):
                maintainer_found = False

                package = _get_action('package_show', {'id': r['package_id']})
                package_maintainer_ids = package['maintainer'].split(',')
                is_hdx = requestdata_helper.is_hdx_portal()

                if is_hdx:
                    # Quick fix for hdx portal
                    maintainer_ids = []
                    for maintainer_name in package_maintainer_ids:
                        try:
                            main_ids =\
                                _get_action('user_show',
                                            {'id': maintainer_name})
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
                            if r['state'] == 'new':
                                org['requests_new'].remove(r)
                            elif r['state'] == 'open':
                                org['requests_open'].remove(r)
                            elif r['state'] == 'archive':
                                org['requests_archive'].remove(r)

            org['requests_archive'] = \
                helpers.group_archived_requests_by_dataset(
                    org['requests_archive'])

            q_org = [x for x in q_organizations if x.get('org') == org['name']]

            if q_org:
                q_org = q_org[0]
                order = q_org.get('order')
                reverse = q_org.get('reverse')
                current_order_name = q_org.get('current_order_name')
            else:
                order = 'last_request_created_at'
                reverse = True
                current_order_name = 'Most Recent'

            org['current_order_name'] = current_order_name

            if order == 'last_request_created_at':
                for dataset in org['requests_archive']:
                    created_at = \
                        dataset.get('requests_archived')[0].get('created_at')
                    data = {
                        'last_request_created_at': created_at
                    }
                    dataset.update(data)

            org['requests_archive'] = \
                sorted(org['requests_archive'],
                       key=lambda x: x[order],
                       reverse=reverse)

        organizations_for_filters = \
            sorted(organizations_for_filters.iteritems(),
                   key=lambda (x, y): y['requests'], reverse=True)

        total_requests_counters =\
            _get_action('requestdata_request_data_counters_get_all', {})
        extra_vars = {
            'organizations': organizations,
            'organizations_for_filters': organizations_for_filters,
            'total_requests_counters': total_requests_counters
        }

        return toolkit.render('admin/all_requests_data.html', extra_vars)

    def download_requests_data(self):
        '''
            Handles creating csv or json file from all of the Requested Data

            :returns: json or csv file
        '''

        file_format = request.query_string
        requests = \
            _get_action('requestdata_request_list_for_sysadmin', {})
        s = StringIO()

        if 'json' in file_format.lower():
            response.headerlist = \
                [('Content-Type', 'application/json'),
                 ('Content-Disposition',
                  'attachment;filename="data_requests.json"')]
            json.dump(requests, s, indent=4)

            return s.getvalue()

        if 'csv' in file_format.lower():
            response.headerlist = \
                [('Content-Type', 'text/csv'),
                 ('Content-Disposition',
                  'attachment;filename="data_requests.csv"')]
            writer = csv.writer(s, encoding='utf-8')
            header = True
            for k in requests:
                if header:
                    writer.writerow(k.keys())
                    header = False
                writer.writerow(k.values())

            return s.getvalue()
