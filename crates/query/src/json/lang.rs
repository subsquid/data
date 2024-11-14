use crate::json::exp::Exp;
use crate::primitives::Name;


#[derive(Debug, Clone)]
pub struct JsonObject {
    props: Vec<(Name, Exp)>
}


impl Default for JsonObject {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject {
    pub fn new() -> Self {
        Self {
            props: Vec::new()
        }
    }

    pub fn add<E: Into<Exp>>(&mut self, name: Name, exp: E) -> &mut Self {
        self.props.push((name, exp.into()));
        self
    }

    pub fn merge(&mut self, other: JsonObject) -> &mut Self {
        self.props.extend(other.props);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.props.len() == 0
    }
}


impl From<JsonObject> for Exp {
    fn from(val: JsonObject) -> Self {
        Exp::Object(val.props)
    }
}


macro_rules! json_object {
    (
        $({
            $( $fields:ident . $fields_prop:ident, )*
            $( [$exp_fields:ident . $exp_fields_prop:ident]: $exp:ident, )*
            $( |$obj:ident| $cb:expr ),*
        }),*
    ) => {{
        use identconv::camel_strify;

        macro_rules! trim_r {
            ($name:expr) => {{
                $name.strip_prefix("r#").unwrap_or($name)
            }};
        }

        let mut object = JsonObject::new();
        $(
            $(
                if $fields.$fields_prop {
                    let prop = trim_r!(camel_strify!($fields_prop));
                    let column = trim_r!(stringify!($fields_prop));
                    object.add(prop, Exp::Prop(column, Box::new(Exp::Value)));
                }
            )*
            $(
                if $exp_fields.$exp_fields_prop {
                    let prop = trim_r!(camel_strify!($exp_fields_prop));
                    let column = trim_r!(stringify!($exp_fields_prop));
                    object.add(prop, Exp::Prop(column, Box::new(Exp::$exp)));
                }
            )*
            $(
                {
                    let $obj = &mut object;
                    $cb
                };
            )*
        )*
        object
    }};
}
pub(crate) use json_object;


pub fn roll(exp: Exp, columns: Vec<Name>) -> Exp {
    Exp::Roll {
        columns,
        exp: Box::new(exp)
    }
}


pub fn prop<E: Into<Exp>>(name: Name, exp: E) -> Exp {
    Exp::Prop(name, Box::new(exp.into()))
}