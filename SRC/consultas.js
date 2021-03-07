/*CONSULTAS SIMPLES*/

/*1. En esta primera consulta nos planteamos obtener la información
de cada una de las personas registradas en la base de datos.
Visualizaremos la información de la BD personas, las redes sociales que
tiene cada una de ellas junto con los detalles de dicha red social
 y el barrio donde vive dicha persona junto con algunos detalles del barrio*/

db.Personas.aggregate(
    [
        {
            $lookup: {
                from: "barrios",
                localField: "CodPost",
                foreignField: "CodPost",
                as: "DetallesUsuario"
            }
        },
        {
            $lookup: {
                from: "redesSS",
                localField: "CodRedSS",
                foreignField: "CodRedSS",
                as: "RedesSociales"
            }
        }
    ]
).pretty()

/*2. En esta consulta nos planteamos el objetivo de recuperar el nombre 
y apellidos del usuario, redes sociales a las que
está suscrito, ciudad en la que vive y nombres de usuario que utiliza*/

db.Personas.aggregate(
    [
        {
            $lookup: {
                from: "barrios",
                localField: "CodPost",
                foreignField: "CodPost",
                as: "DetallesUsuario"
            }
        },
        {
            $lookup: {
                from: "redesSS",
                localField: "CodRedSS",
                foreignField: "CodRedSS",
                as: "RedesSociales"
            }
        },
        {
            $project: {
                _id: 0,
                Nombre: 1,
                Apellidos: 1,
                NombreUsuarios: 1,
                NombreRedSocial: "$RedesSociales.Nombre",
                NombreCiudad: "$DetallesUsuario.Ciudad"
            }
        }
    ]
).pretty()




/*CONSULTAS COMPLEJAS*/

/*1. En esta consulta el objetivo es saber cuanto les cuesta mensualmente
a cada usuario el total de las cuotas de las redes sociales en las que están
inscritos, además de comparar las horas de uso que hace el usuario con
las horas de uso que cada una de las redes sociales estima que cada usuario hace
de media. Una vez sepamos los datos referentes a las horas de uso clasificaremos a los usuarios
en aquellos que hacen un uso por debajo de la media, los que hacen uso igual al estimado en la media y
los que hacen uso por encima de la media*/

db.Personas.aggregate(
    [
        {
            $lookup: {
                from: "redesSS",
                localField: "CodRedSS",
                foreignField: "CodRedSS",
                as: "RedesSociales"
            }
        },
        {
            $unwind: "$RedesSociales"
        },
        {
            $project: {
                _id: 0,
                Identificador: "$_id",
                Nombre: "$Nombre",
                Apellidos: "$Apellidos",
                HorasDeUsoDiario: "$HorasDeUso",
                NombreRedSocial: "$RedesSociales.Nombre",
                HorasMediasDiarias: "$RedesSociales.horasUsoMedioXdia",
                CuotaMensual: "$RedesSociales.cuotaMensual"
            }
        },
        {
            $group: {
                _id: {
                    Identificador: "$Identificador",
                    Nombre: "$Nombre",
                    Apellidos: "$Apellidos"
                },
                CuotaMensual: { $sum: "$CuotaMensual" },
                totalHorasDeUsoDiario: { $avg: "$HorasDeUsoDiario" },
                MediaUsoDiario: { $avg: "$HorasMediasDiarias" }
            }
        },
        {
            $project: {
                CuotaMensual: 1,
                totalHorasDeUsoDiario: 1,
                MediaUsoDiario: 1,
                ComparacionHoras: { $cmp: ["$totalHorasDeUsoDiario", "$MediaUsoDiario"] }
            }
        },
        {
            $bucket: {
                groupBy: "$ComparacionHoras",
                boundaries: [-1, 0, 1, 1.1],
                output: {
                    "total": { $sum: 1 },
                    "Usuarios":
                    {
                        $push: {
                            "name": { $concat: ["$_id.Nombre", " ", "$_id.Apellidos"] },
                            "Cuota Mensual": "$CuotaMensual",
                            "totalHorasDeUsoDiario": "$totalHorasDeUsoDiario",
                            "MediaUsoDiario": "$MediaUsoDiario"
                        }
                    }
                }
            }
        }]).pretty()

/*2. En esta segunda consulta nos planteamos el objetivo de obtener 
un listado en el que se ofrezca la siguiente información: Nombre y apellidos de los usuarios,
 nombres de usuario, redes sociales a las que pertenecen, en el caso de que paguen cuanto pagan al mes, 
ciudad en la que viven, barrio y nombre de la calle en  el caso de que el usuario lo haya especificado 
y en último lugar la renta media del barrio en el que vivan, 
para la que habra que crear un nuevo campo en cada documento en función 
 de que sea una renta alta (>1500€), renta media (>1000€, <1500€) o renta baja (<=1000€), dicho campo se llamará 
 resultados y especificara la evaluación de la renta.
  Para terminar se creara una colección con los resultados arrojados por la 
  consulta la cual se llamara "resultadosCONSULTA".
CONDICIONES: 
        + Seleccionar únicamente usuarios que hayan ingresado a una red social 
        por primera vez despues del 1 de enero del 2015.
        + Que hayan realizado al menos 1 publicación.
        + Que residan en barrios con almenos 300 habitantes*/

db.Personas.aggregate([
    {
        $match: {
            "FechaIngreso": {
                $gt: new ISODate("2015-01-01")
            }
        }
    },
    {
        $match: {
            "Publicaciones": {
                $gte: 1
            }
        }
    },
    {
        $lookup: {
            from: "barrios",
            localField: "CodPost",
            foreignField: "CodPost",
            as: "DetallesResidencia"
        }
    },
    {
        $match: {
            "DetallesResidencia.Habitantes": {
                $gte: 300
            }
        }
    },
    {
        $lookup: {
            from: "redesSS",
            localField: "CodRedSS",
            foreignField: "CodRedSS",
            as: "RedesSociales"
        }
    },
    {
        $project: {
            _id: 0,
            Nombre: "$Nombre",
            Apellidos: "$Apellidos",
            Usuarios: "$NombresUsuarios",
            NombreRedSocial: "$RedesSociales.Nombre",
            CuotaMensual: { $sum: "$RedesSociales.cuotaMensual" },
            Ciudad: "$DetallesResidencia.Ciudad",
            Barrio: "$DetallesResidencia.Nombre",
            Calle: "$Direccion",
            RentaMediaBarrio: "$DetallesResidencia.rentaMedia",
        }
    },
    {
        $set: {
            RentaMediaBarrio: { $arrayElemAt: ["$RentaMediaBarrio", 0] }
        }
    },
    {
        $project: {
            Nombre: 1,
            Apellidos: 1,
            Usuarios: 1,
            NombreRedSocial: 1,
            CuotaMensual: 1,
            Ciudad: 1,
            Barrio: 1,
            Calle: 1,
            RentaMediaBarrio: 1,
            Resultados:
            {
                $switch: {
                    branches: [
                        { case: { $lte: ["$RentaMediaBarrio", 1000] }, then: "Renta Baja" },
                        {
                            case: {
                                $and:
                                    [{ $gt: ["$RentaMediaBarrio", 1000] },
                                    { $lt: ["$RentaMediaBarrio", 1500] }]
                            },
                            then: "Renta Media"
                        },
                        { case: { $gt: ["$RentaMediaBarrio", 1500] }, then: "Renta Alta" }
                    ]
                }
            }
        }
    },
    { $sort: { RentaMediaBarrio: 1 } },
    { $merge: { into: "resultadosCONSULTA" } }
]).pretty()

/*3. En la siguiente consulta nos planteamos el objetivo de calificar a los usuarios de las redes sociales en usuarios 
pasivos y usuarios activos, se creará un campo en el que se especifique el tipo de usuarios que es y se crearán dos grupos
en función de la calificación que se le haya dado, para determinar si el usuario es pasivo o activo utilizaremos 
un parametro, el número de horas de uso diario.
Como datos adicionales también se requiere saber el número de días que lleva logeado en las redes social, es decir el 
número de días desde su fecha de ingreso hasta el día actual. También es necesario saber a que redes sociales
 está suscrito el usuario y en que ciudad vive.
*/

db.Personas.aggregate([
    {
        $project: {
            _id: 0,
            Nombre: "$Nombre",
            Apellidos: "$Apellidos",
            FechaIngreso: "$FechaIngreso",
            DiasDesdeLogOn: {
                $trunc: {
                    $divide: [{ $subtract: [new Date(), '$FechaIngreso'] }, 1000 * 60 * 60 * 24]
                }
            },
            Publicaciones: "$Publicaciones",
            HorasDeUso: "$HorasDeUso",
            CodPost: "$CodPost",
            CodRedSS: "$CodRedSS",
            tipoDeUsuario:
            {
                $switch: {
                    branches: [
                        { case: { $gte: ["$HorasDeUso", 2] }, then: "Usuario Activo" },
                        { case: { $lt: ["$HorasDeUso", 2] }, then: "Usuario Pasivo" }
                    ]
                }
            }
        }
    },
    {
        $lookup: {
            from: "barrios",
            localField: "CodPost",
            foreignField: "CodPost",
            as: "DetallesResidencia"
        }
    },
    {
        $lookup: {
            from: "redesSS",
            localField: "CodRedSS",
            foreignField: "CodRedSS",
            as: "RedesSociales"
        }
    },
    {
        $bucket: {
            groupBy: "$HorasDeUso",
            boundaries: [0, 2, 24],
            default: "No hacen uso",
            output: {
                "total": { $sum: 1 },
                "Usuarios":
                {
                    $push: {
                        "NombreCompleto": { $concat: ["$Nombre", " ", "$Apellidos"] },
                        "FechaIngreso": "$FechaIngreso",
                        "DiasDesdeLogOn": "$DiasDesdeLogOn",
                        "tipoDeUsuario": "$tipoDeUsuario",
                        "RedesSociales": "$RedesSociales.Nombre",
                        "Ciudad": "$DetallesResidencia.Ciudad"
                    }
                }
            }
        }
    }
]).pretty()


/*4.- En esta consulta nos planteamos el objetivo de mediante los datos de la colección redes sociales realizar 
sobre el porcentaje de usuarios de baja y de alta al día en cuanto al total de usuarios que tiene la página,
también se necesita crear un campo llamado membresía en el que se especifique si la página es de pago o gratuita.
Una vez se obtengan todos estos datos se tendrá que exportar los resultados a una base de datos y 
colección concreto y con ello crear una tabla de excel en la que se muestren los resultados obtenidos. */

db.redesSS.aggregate([
    { $unset: ["cuotaMensual", "horasUsoMedioXdia", "fechaCreacion"] },
    {
        $project:
        {
            CodRedSS: 1,
            Nombre: 1,
            numeroUsuarios: 1,
            PorcentajeAltasDiarias: {
                $trunc: [{
                    $multiply: [{ $divide: ["$usuariosNuevosXdia", "$numeroUsuarios"] }, 100]
                }, 4]
            },
            PorcentajeBajasDiarias: {
                $trunc: [{
                    $multiply: [{ $divide: ["$usuariosDeBajaXdia", "$numeroUsuarios"] }, 100]
                }, 4]
            },
            Membresia:
            {
                $cond: { if: { $eq: ["$DePago", true] }, then: "Pago mensual", else: "Gratuita" }
            }
        }
    },
    { $unset: ["_id"] },
    { $out: { db: "Consulta4", coll: "EstudioPorcentajes" } }
]).pretty()
