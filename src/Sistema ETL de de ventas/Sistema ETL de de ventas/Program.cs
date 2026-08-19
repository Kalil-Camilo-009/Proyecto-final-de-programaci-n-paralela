using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Sistema_ETL_de_de_ventas
{
    // Modelo de la entidad de venta para la simulación masiva
    public class Venta
    {
        public int Id { get; set; }
        public string Sucursal { get; set; }
        public double MontoBase { get; set; }
        public double Impuesto { get; set; }
        public double Total { get; set; }
    }

    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "Sistema ETL Paralelo de Ventas - ITLA";

            int maxProcessors = Environment.ProcessorCount;
            Console.WriteLine("=================================================================");
            Console.WriteLine("    SISTEMA ETL PARALELO DE VENTAS MULTISUCURSAL (C# / .NET)     ");
            Console.WriteLine("=================================================================");
            Console.WriteLine($"Procesadores lógicos disponibles en el equipo: {maxProcessors}");

            Console.Write("\nIngrese la cantidad de procesadores a utilizar: ");
            int numProcesadores = int.Parse(Console.ReadLine());
            if (numProcesadores > maxProcessors)
            {
                numProcesadores = maxProcessors;
                Console.WriteLine($"-> Ajustado al máximo permitido: {numProcesadores} procesadores.");
            }

            Console.Write("Ingrese la cantidad de registros masivos a simular (ej. 10000000): ");
            int totalRegistros = int.Parse(Console.ReadLine());

            Console.WriteLine("\n[1/3] Generando transacciones masivas en memoria...");
            Venta[] dataset = GenerarVentasSinteticas(totalRegistros);

            Console.WriteLine("[2/3] Ejecutando procesamiento ETL (Secuencial vs Paralelo)...");

            double totalSecuencial = 0;
            double totalParBloques = 0;
            double totalParLocales = 0;

            // --- MEDICIÓN SECUENCIAL ---
            Stopwatch swSec = Stopwatch.StartNew();
            for (int i = 0; i < dataset.Length; i++)
            {
                double itbis = dataset[i].MontoBase * 0.18;
                totalSecuencial += dataset[i].MontoBase + itbis;
            }
            swSec.Stop();

            // --- MEDICIÓN PARALELA 1 (Descomposición de Datos de Entrada por Bloques) ---
            Stopwatch swParBloques = Stopwatch.StartNew();
            totalParBloques = await ProcesarETLDescomposicionDatos(dataset, numProcesadores);
            swParBloques.Stop();

            // --- MEDICIÓN PARALELA 2 (Optimización con Datos Locales por Hilo - Parallel.For) ---
            Stopwatch swParLocales = Stopwatch.StartNew();
            totalParLocales = ProcesarETLConDatosLocales(dataset, numProcesadores);
            swParLocales.Stop();

            // --- CÁLCULO DE MÉTRICAS ---
            double tSec = swSec.Elapsed.TotalMilliseconds;
            double tParBloques = swParBloques.Elapsed.TotalMilliseconds;
            double tParLocales = swParLocales.Elapsed.TotalMilliseconds;

            // Métricas Bloques
            double speedupBloques = tParBloques > 0 ? tSec / tParBloques : 0;
            double eficienciaBloques = (speedupBloques / numProcesadores) * 100;
            bool coincidenBloques = Math.Abs(totalSecuencial - totalParBloques) / totalSecuencial < 1e-4;

            // Métricas Datos Locales
            double speedupLocales = tParLocales > 0 ? tSec / tParLocales : 0;
            double eficienciaLocales = (speedupLocales / numProcesadores) * 100;
            bool coincidenLocales = Math.Abs(totalSecuencial - totalParLocales) / totalSecuencial < 1e-4;

            Console.WriteLine("\n=================================================================");
            Console.WriteLine("                    RESULTADOS Y TELEMETRÍA                      ");
            Console.WriteLine("=================================================================");
            Console.WriteLine($"Tiempo Secuencial:                          {tSec:F2} ms");
            Console.WriteLine("-----------------------------------------------------------------");
            Console.WriteLine("MÉTODO 1: Descomposición de Datos (Bloques)");
            Console.WriteLine($"Tiempo Paralelo:                            {tParBloques:F2} ms");
            Console.WriteLine($"Speedup (Aceleración):                      {speedupBloques:F2}x");
            Console.WriteLine($"Eficiencia:                                 {eficienciaBloques:F2}%");
            Console.WriteLine($"¿Resultados coinciden?:                     {coincidenBloques}");
            Console.WriteLine("-----------------------------------------------------------------");
            Console.WriteLine("MÉTODO 2: Optimización con Datos Locales (Parallel.For) [Recomendado]");
            Console.WriteLine($"Tiempo Paralelo:                            {tParLocales:F2} ms");
            Console.WriteLine($"Speedup (Aceleración):                      {speedupLocales:F2}x");
            Console.WriteLine($"Eficiencia:                                 {eficienciaLocales:F2}%");
            Console.WriteLine($"¿Resultados coinciden?:                     {coincidenLocales}");
            Console.WriteLine("=================================================================");

            Console.WriteLine("\nProceso finalizado. Presione cualquier tecla para salir...");
            if (!Console.IsInputRedirected)
            {
                Console.ReadKey();
            }
        }

        // Método de Descomposición de Datos (Particionamiento por Bloques / Chunks Continuos)
        private static async Task<double> ProcesarETLDescomposicionDatos(Venta[] ventas, int numProcesadores)
        {
            int numBloques = numProcesadores;
            int chunkSize = ventas.Length / numBloques;
            if (chunkSize == 0) chunkSize = 1;

            var tareas = new Task<double>[numBloques];

            for (int t = 0; t < numBloques; t++)
            {
                int taskIndex = t;
                int start = taskIndex * chunkSize;
                int end = (taskIndex == numBloques - 1) ? ventas.Length : (taskIndex + 1) * chunkSize;
                int count = end - start;

                // Creamos una partición lógica y contigua en memoria de los datos
                var particion = new ArraySegment<Venta>(ventas, start, count);

                // Entregamos la partición explícitamente a la tarea/hilo
                tareas[taskIndex] = Task.Run(() =>
                {
                    double sumaParcial = 0;
                    // El hilo trabaja de forma secuencial y contigua en su propio bloque asignado
                    for (int i = 0; i < particion.Count; i++)
                    {
                        var venta = particion.Array[particion.Offset + i];
                        double itbis = venta.MontoBase * 0.18;
                        sumaParcial += venta.MontoBase + itbis;
                    }
                    return sumaParcial;
                });
            }

            double[] sumasParciales = await Task.WhenAll(tareas);
            return sumasParciales.Sum();
        }

        // Método optimizado que sigue la recomendación del profesor
        // Evita contención usando almacenamiento local por hilo y consolidando al final
        private static double ProcesarETLConDatosLocales(Venta[] ventas, int numProcesadores)
        {
            object lockObject = new object();
            double totalParalelo = 0;

            var opciones = new ParallelOptions
            {
                MaxDegreeOfParallelism = numProcesadores
            };

            Parallel.For(
                0, 
                ventas.Length, 
                opciones,
                // 1. Inicializador de datos locales del hilo: cada hilo inicia su propia suma en 0.0
                () => 0.0,
                // 2. Cuerpo del bucle: cada hilo procesa sus elementos de forma independiente en su variable local 'localSum'
                (i, state, localSum) =>
                {
                    double itbis = ventas[i].MontoBase * 0.18;
                    return localSum + ventas[i].MontoBase + itbis;
                },
                // 3. Consolidación final: cuando un hilo termina, acumula su suma local en la variable compartida de manera segura
                localSum =>
                {
                    lock (lockObject)
                    {
                        totalParalelo += localSum;
                    }
                }
            );

            return totalParalelo;
        }

        // Generador sintético de ventas multisucursal en memoria
        private static Venta[] GenerarVentasSinteticas(int cantidad)
        {
            string[] sucursales = { "Santo Domingo", "Santiago", "La Vega", "Puerto Plata" };
            Random rnd = new Random();
            Venta[] ventas = new Venta[cantidad];

            for (int i = 0; i < cantidad; i++)
            {
                ventas[i] = new Venta
                {
                    Id = i + 1,
                    Sucursal = sucursales[i % sucursales.Length],
                    MontoBase = rnd.Next(100, 5000)
                };
            }
            return ventas;
        }
    }
}