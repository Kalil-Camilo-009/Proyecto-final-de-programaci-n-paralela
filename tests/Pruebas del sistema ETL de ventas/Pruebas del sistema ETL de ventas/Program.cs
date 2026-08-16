using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Pruebas_del_sistema_ETL_de_ventas
{
    // Modelo de datos para las pruebas del ETL
    public class VentaTest
    {
        public int Id { get; set; }
        public string Sucursal { get; set; }
        public double MontoBase { get; set; }
        public double Impuesto { get; set; }
        public double Total { get; set; }
    }

    // Interfaz para la ejecución modular de pruebas
    public interface IOperation
    {
        Task Iniciar();
    }

    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "Suite de Pruebas ETL Paralelo - ITLA";

            // Para cambiar de prueba, simplemente instanciar la clase deseada:
            // -----------------------------------------------------------------
             //IOperation operacion = new PruebasETL.Prueba1_ParticionamientoSalida_ParallelFor();
            //IOperation operacion = new PruebasETL.Prueba2_ParticionamientoEntrada_GranoGruesoMIMD();
            IOperation operacion = new PruebasETL.Prueba3_BenchmarkingMultiMetodo_Speedup();

            await operacion.Iniciar();

            Console.WriteLine("\nPresione cualquier tecla para finalizar las pruebas...");
            Console.ReadKey();
        }
    }

    internal class PruebasETL
    {
        // ----------------------------------------------------------------------------------
        // PRUEBA 1: Descomposición de Datos por Salida (Parallel.For)
        // ----------------------------------------------------------------------------------
        public class Prueba1_ParticionamientoSalida_ParallelFor : IOperation
        {
            public async Task Iniciar()
            {
                Console.WriteLine("=== PRUEBA 1: Particionamiento de Datos de Salida (Parallel.For) ===");
                int N = 10_000_000;
                VentaTest[] datos = GenerarDatos(N);
                VentaTest[] resultado = new VentaTest[N];

                Stopwatch sw = Stopwatch.StartNew();
                Parallel.For(0, N, i =>
                {
                    double itbis = datos[i].MontoBase * 0.18;
                    resultado[i] = new VentaTest
                    {
                        Id = datos[i].Id,
                        Sucursal = datos[i].Sucursal,
                        MontoBase = datos[i].MontoBase,
                        Impuesto = itbis,
                        Total = datos[i].MontoBase + itbis
                    };
                });
                sw.Stop();

                Console.WriteLine($"Procesados {N:N0} registros de salida.");
                Console.WriteLine($"Tiempo de ejecución paralelo: {sw.ElapsedMilliseconds} ms");
            }
        }

        // ----------------------------------------------------------------------------------
        // PRUEBA 2: Descomposición de Datos por Entrada (Grano Grueso / MIMD con Tasks)
        // ----------------------------------------------------------------------------------
        public class Prueba2_ParticionamientoEntrada_GranoGruesoMIMD : IOperation
        {
            public async Task Iniciar()
            {
                Console.WriteLine("=== PRUEBA 2: Particionamiento de Entrada (Grano Grueso - Arquitectura MIMD) ===");
                int N = 20_000_000;
                VentaTest[] datos = GenerarDatos(N);

                int numProcesadores = Environment.ProcessorCount;
                int chunkSize = N / numProcesadores;
                var tareas = new Task<double>[numProcesadores];

                Stopwatch sw = Stopwatch.StartNew();

                for (int t = 0; t < numProcesadores; t++)
                {
                    int taskIndex = t;
                    int start = taskIndex * chunkSize;
                    int end = (taskIndex == numProcesadores - 1) ? N : (taskIndex + 1) * chunkSize;

                    // Cada tarea ejecuta una instrucción/bucle autónomo sobre un subrango de datos (MIMD)
                    tareas[taskIndex] = Task.Run(() =>
                    {
                        double sumaLocal = 0;
                        for (int i = start; i < end; i++)
                        {
                            sumaLocal += datos[i].MontoBase * 1.18;
                        }
                        return sumaLocal;
                    });
                }

                double[] resultadosParciales = await Task.WhenAll(tareas);
                double granTotal = resultadosParciales.Sum();
                sw.Stop();

                Console.WriteLine($"Total procesado (N={N:N0} en {numProcesadores} tareas MIMD): {granTotal:C2}");
                Console.WriteLine($"Tiempo de ejecución: {sw.ElapsedMilliseconds} ms");
            }
        }

        // ----------------------------------------------------------------------------------
        // PRUEBA 3: Benchmarking Completo con Medición de Speedup, Eficiencia y Threads
        // ----------------------------------------------------------------------------------
        public class Prueba3_BenchmarkingMultiMetodo_Speedup : IOperation
        {
            public async Task Iniciar()
            {
                Console.WriteLine("=== PRUEBA 3: Benchmarking Multi-método (Parallel, Tasks, Threads) ===");
                int maxProcessors = Environment.ProcessorCount;
                Console.WriteLine($"Procesadores del sistema: {maxProcessors}");

                int N = 15_000_000;
                Console.WriteLine($"Generando {N:N0} datos de prueba...");
                VentaTest[] datos = GenerarDatos(N);

                // 1. Ejecución Secuencial
                Stopwatch swSec = Stopwatch.StartNew();
                double totalSecuencial = 0;
                for (int i = 0; i < N; i++)
                {
                    totalSecuencial += datos[i].MontoBase * 1.18;
                }
                swSec.Stop();
                long tSec = swSec.ElapsedMilliseconds;

                // 2. Ejecución con Parallel.For
                Stopwatch swParallel = Stopwatch.StartNew();
                double totalParallel = 0;
                object lockObj = new object();
                Parallel.For(0, N, () => 0.0, (i, state, localSum) =>
                {
                    return localSum + (datos[i].MontoBase * 1.18);
                },
                localSum =>
                {
                    lock (lockObj) { totalParallel += localSum; }
                });
                swParallel.Stop();
                long tParallel = swParallel.ElapsedMilliseconds;

                // 3. Ejecución con Tasks (TPL Grano Grueso)
                Stopwatch swTasks = Stopwatch.StartNew();
                int chunkSize = N / maxProcessors;
                var tasks = new Task<double>[maxProcessors];
                for (int t = 0; t < maxProcessors; t++)
                {
                    int taskIndex = t;
                    int start = taskIndex * chunkSize;
                    int end = (taskIndex == maxProcessors - 1) ? N : (taskIndex + 1) * chunkSize;
                    tasks[taskIndex] = Task.Run(() =>
                    {
                        double sum = 0;
                        for (int i = start; i < end; i++) sum += datos[i].MontoBase * 1.18;
                        return sum;
                    });
                }
                double[] resTasks = await Task.WhenAll(tasks);
                double totalTasks = resTasks.Sum();
                swTasks.Stop();
                long tTasks = swTasks.ElapsedMilliseconds;

                // 4. Ejecución con Threads Nativos
                Stopwatch swThreads = Stopwatch.StartNew();
                double[] resThreads = new double[maxProcessors];
                Thread[] threads = new Thread[maxProcessors];
                for (int t = 0; t < maxProcessors; t++)
                {
                    int threadIndex = t;
                    int start = threadIndex * chunkSize;
                    int end = (threadIndex == maxProcessors - 1) ? N : (threadIndex + 1) * chunkSize;
                    threads[threadIndex] = new Thread(() =>
                    {
                        double sum = 0;
                        for (int i = start; i < end; i++) sum += datos[i].MontoBase * 1.18;
                        resThreads[threadIndex] = sum;
                    });
                    threads[threadIndex].Start();
                }
                foreach (var th in threads) th.Join();
                double totalThreads = resThreads.Sum();
                swThreads.Stop();
                long tThreads = swThreads.ElapsedMilliseconds;

                // Presentación de métricas de telemetría
                Console.WriteLine("\n==========================================================");
                Console.WriteLine("                TABLA RECAPITULATIVA DE PRUEBAS            ");
                Console.WriteLine("==========================================================");
                Console.WriteLine($"Tiempo Secuencial: {tSec} ms");

                ImprimirMetrica("Parallel.For", tSec, tParallel, maxProcessors, Math.Abs(totalSecuencial - totalParallel) < 1e-4);
                ImprimirMetrica("Tasks (TPL)", tSec, tTasks, maxProcessors, Math.Abs(totalSecuencial - totalTasks) < 1e-4);
                ImprimirMetrica("Threads", tSec, tThreads, maxProcessors, Math.Abs(totalSecuencial - totalThreads) < 1e-4);
            }

            private void ImprimirMetrica(string metodo, long tSec, long tPar, int procesadores, bool coincide)
            {
                double speedup = tPar > 0 ? (double)tSec / tPar : 0;
                double eficiencia = (speedup / procesadores) * 100;
                Console.WriteLine($"\nMétodo: {metodo}");
                Console.WriteLine($"  Tiempo: {tPar} ms");
                Console.WriteLine($"  Speedup: {speedup:F2}x");
                Console.WriteLine($"  Eficiencia: {eficiencia:F2}%");
                Console.WriteLine($"  Resultados coinciden: {coincide}");
            }
        }

        // Generador auxiliar de registros de prueba
        private static VentaTest[] GenerarDatos(int cantidad)
        {
            string[] sucursales = { "Santo Domingo", "Santiago", "La Vega", "Puerto Plata" };
            Random rnd = new Random(42); // Semilla fija para consistencia en pruebas
            VentaTest[] datos = new VentaTest[cantidad];
            for (int i = 0; i < cantidad; i++)
            {
                datos[i] = new VentaTest
                {
                    Id = i + 1,
                    Sucursal = sucursales[i % sucursales.Length],
                    MontoBase = rnd.Next(100, 5000)
                };
            }
            return datos;
        }
    }
}