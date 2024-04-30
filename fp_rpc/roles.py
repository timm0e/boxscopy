import inspect

import fp_rpc.rpc as rpc


ALL_ROLES: set["Role"] = set()
SELF_ROLE: "Role | None" = None


def get_role_by_name(name: str) -> "Role | None":
    for role in ALL_ROLES:
        if role.name == name:
            return role
    return None


def get_current_role() -> "Role | None":
    return SELF_ROLE


def set_current_role(role: "Role | None"):
    global SELF_ROLE
    SELF_ROLE = role


class Role:
    def __init__(self, name: str):
        if not name:
            raise ValueError("Role name cannot be empty")
        self.name = name
        ALL_ROLES.add(self)

    def __call__(self, func):
        funcname = f"{func.__qualname__}@{self.name}"
        rpc.register_func(funcname, (func, self))

        def rpc_shim(*args, **kwargs):
            role = get_current_role()
            if role is None:
                return rpc.RPC_CLIENT.rpc_call(funcname, *args, **kwargs)

            assert (
                role == self
            ), f"Function {funcname} is only meant to be called on {self}, not {role}!"
            return func(*args, **kwargs)

        return rpc_shim

    def __str__(self):
        return f"<Role: {self.name}>"


class StatefulRole:
    """
    ClassRoles function similarly to Roles, but are meant to be subclassed to house some sort of state.
    This state is not meant to be accessible from the controller, and is therefore not meant to be networked per default.

    If you want to share some state with the controller, you need to create explicit getter/setter methods.
    To keep the state only on the worker nodes, the `self` parameter of functions gets mangled:
    * A StateRole class is only a housing for state, only by instantiating it an actual Role gets created
    * Each instance gets stored inside the class's lookup table
    * When calling from the controller, `self` gets replaced by the (unique) role name
    * On the worker, the role name gets looked up to the state instance, which then gets passed into the functions `self` parameter
    """

    role: Role
    instances: dict[str, "StatefulRole"]

    def __init__(self, name: str):
        self.role = Role(name)
        self._wrap_extra_methods()

    # TODO: duplicate registrations are now avoided, but we still need to do the dance to avoid networking `self`!
    # To do that, just don't send the `self` argument over the network,
    # Wait, actually, this should just work, since the method.__self__ attribute is set on the method instance
    # See https://docs.python.org/3/reference/datamodel.html#instance-methods

    def _wrap_extra_methods(self):
        """
        Wrap all non-private methods in this class with the RPC shim.
        This is primarily used for subclassing Role for it to house internal state.

        Methods considered private are per convention those that start with an underscore.
        This therefore also includes "dunder" methods.
        :return:
        """
        for key in self.__class__.__dict__:
            # Methods are only defined on classes
            # As per 3.2.8.2. Instance methods, the conversion from function -> bound method only happens whenever
            # the attribute is retrieved from the instance. The docs also mention that then saving the bound method
            # to a (local) variable can be an optimization technique - therefore the following should be fine
            if key.startswith("_"):
                continue
            value = getattr(self, key)
            if inspect.ismethod(value):
                setattr(self, key, self.role(value))

    def __call__(self, *args, **kwargs):
        return self.role(*args, **kwargs)