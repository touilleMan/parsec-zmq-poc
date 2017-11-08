async def cmd_REGISTER(app, req):
    pass


async def cmd_LOGIN(app, req):
    pass


async def cmd_GET_AVAILABLE_LOGINS(app, req):
    pass


async def cmd_GET_CORE_STATE(app, req):
    pass


async def cmd_LOGOUT(app, req):
    pass


async def cmd_FILE_CREATE(app, req):
    pass


async def cmd_FILE_READ(app, req):
    pass


async def cmd_FILE_WRITE(app, req):
    pass


async def cmd_STAT(app, req):
    pass


async def cmd_FOLDER_CREATE(app, req):
    pass


async def cmd_MOVE(app, req):
    pass


async def cmd_DELETE(app, req):
    pass


async def cmd_FILE_TRUNCATE(app, req):
    pass






async def _cmd_LOGIN

class Control:
    def __init__(self):
        self.user_pubkey = None
        self.user_privkey = None
        self.fs = None

    def init_app(self, app):
        self.app = app
        app.extensions['logged_user'] = None
        app.extensions['control'] = self

        app.register_cmd('register', self._cmd_REGISTER)
        app.register_cmd('login', self._cmd_LOGIN)
        app.register_cmd('get_available_logins', self._cmd_GET_AVAILABLE_LOGINS)
        app.register_cmd('get_core_state', self._cmd_GET_CORE_STATE)
        app.register_cmd('logout', self._cmd_LOGOUT)

        # FS api
        app.register_cmd('file_create', self._fs_proxy_factory('file_create'))
        app.register_cmd('file_read', self._fs_proxy_factory('file_read'))
        app.register_cmd('file_write', self._fs_proxy_factory('file_write'))
        app.register_cmd('stat', self._fs_proxy_factory('stat'))
        app.register_cmd('folder_create', self._fs_proxy_factory('folder_create'))
        app.register_cmd('move', self._fs_proxy_factory('move'))
        app.register_cmd('delete', self._fs_proxy_factory('delete'))
        app.register_cmd('file_truncate', self._fs_proxy_factory('file_truncate'))

    def _fs_proxy_factory(self, cmd):
        def proxy_cmd(app, req):
            if not app.extensions['logged_user']:
                raise ParsecError('not_logged', 'Must be logged in to use this command')
            else:
                return getattr(self.fs, '_cmd_%s' % cmd.upper())(app, req)

        return proxy_cmd

    def _cmd_REGISTER(self, app, req):
        raise NotImplementedError()
        return {'status': 'ok'}

    def _cmd_LOGIN(self, app, req):
        msg = cmd_LOGIN_Schema().load(req.msg)
        try:
            userid, _ = self.app.extensions['logged_user']
        except TypeError:
            pass
        else:
            raise ParsecError('already_logged', 'Already logged in as `%s`' % userid)
        userkeys = self.app.config['GET_USER'](msg['id'], msg['password'])
        if not userkeys:
            raise ParsecError('unknown_user', 'No user known with id `%s`' % msg['id'])
        # TODO: use try/except to avoid inconsistant state on init crash ?
        self.app.extensions['logged_user'] = (msg['id'], userkeys)
        self.backend_connection = BackendConnection(self.app)
        self.backend_connection.start()
        # TODO: check connection to the backend ?
        self.fs = FSPipeline(self.app)
        self.fs.start()
        return {'status': 'ok'}

    def _cmd_GET_AVAILABLE_LOGINS(self, app, req):
        raise NotImplementedError()
        return {'status': 'ok'}

    def _cmd_GET_CORE_STATE(self, app, req):
        try:
            userid, _ = self.app.extensions['logged_user']
            # Create a socket connection just for this
            sock = self.app.zmqcontext.socket(zmq.REQ)
            sock.setsockopt(
                zmq.CURVE_SERVERKEY, b64_to_z85(self.app.config['SERVER_PUBLIC']))
            sock.setsockopt(
                zmq.CURVE_PUBLICKEY, b64_to_z85(self.app.config['ANONYMOUS_PUBKEY']))
            sock.setsockopt(
                zmq.CURVE_SECRETKEY, b64_to_z85(self.app.config['ANONYMOUS_PRIVKEY']))
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self.app.config['BACKEND_URL'])
            sock.send_json({"cmd": "ping"})
            poller = zmq.Poller()
            poller.register(sock, zmq.POLLIN)
            if poller.poll(1000):
                rep = sock.recv_json()
                online = (rep == {'status': 'ok'})
            else:  # Timeout
                online = False
        except Exception:
            userid = None
            online = False
        return {'status': 'ok', 'online': online, 'logged': userid}

    def _cmd_LOGOUT(self, app, req):
        if not self.app.extensions['logged_user']:
            raise ParsecError('not_logged', 'Must be logged in to use this command')
        self.fs.stop()
        self.backend_connection.stop()
        self.app.extensions['logged_user'] = None
        return {'status': 'ok'}


control = Control()
init_control_api = control.init_app
