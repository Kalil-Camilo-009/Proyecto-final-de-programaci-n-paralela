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
            double totalParalelo = 0;

            // --- MEDICIÓN SECUENCIAL ---
            Stopwatch swSec = Stopwatch.StartNew();
            for (int i = 0; i < dataset.Length; i++)
            {
                double itbis = dataset[i].MontoBase * 0.18;
                totalSecuencial += dataset[i].MontoBase + itbis;
            }
            swSec.Stop();

            // --- MEDICIÓN PARALELA (Descomposición de Datos de Entrada por Bloques) ---
            Stopwatch swPar = Stopwatch.StartNew();
            totalParalelo = await ProcesarETLDescomposicionDatos(dataset, numProcesadores);
            swPar.Stop();

            // --- CÁLCULO DE MÉTRICAS ---
            long tSec = swSec.ElapsedMilliseconds;
            long tPar = swPar.ElapsedMilliseconds;

            double speedup = tPar > 0 ? (double)tSec / tPar : 0;
            double eficiencia = (speedup / numProcesadores) * 100;
            bool coinciden = Math.Abs(totalSecuencial - totalParalelo) < 1e-5;

            Console.WriteLine("\n=================================================================");
            Console.WriteLine("                    RESULTADOS Y TELEMETRÍA                      ");
            Console.WriteLine("=================================================================");
            Console.WriteLine($"Tiempo Secuencial: {tSec} ms");
            Console.WriteLine($"Tiempo Paralelo:   {tPar} ms");
            Console.WriteLine($"Speedup (Aceleración): {speedup:F2}x");
            Console.WriteLine($"Eficiencia:            {eficiencia:F2}%");
            Console.WriteLine($"¿Resultados coinciden?: {coinciden}");
            Console.WriteLine("=================================================================");

            Console.WriteLine("\nProceso finalizado. Presione cualquier tecla para salir...");
            Console.ReadKey();
        }

        // Método de Descomposición de Datos (Particionamiento por Bloques / Chunks)
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

                tareas[taskIndex] = Task.Run(() =>
                {
                    double sumaParcial = 0;
                    for (int i = start; i < end; i++)
                    {
                        double itbis = ventas[i].MontoBase * 0.18;
                        sumaParcial += ventas[i].MontoBase + itbis;
                    }
                    return sumaParcial;
                });
            }

            double[] sumasParciales = await Task.WhenAll(tareas);
            return sumasParciales.Sum();
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