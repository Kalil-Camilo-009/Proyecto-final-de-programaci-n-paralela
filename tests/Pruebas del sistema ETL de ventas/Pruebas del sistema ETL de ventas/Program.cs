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
            // IOperation operacion = new PruebasETL.Prueba1_ParticionamientoSalida_ParallelFor();
            // IOperation operacion = new PruebasETL.Prueba2_ParticionamientoEntrada_GranoGruesoMIMD();
            IOperation operacion = new PruebasETL.Prueba3_BenchmarkingMultiMetodo_Speedup();
            await operacion.Iniciar();
            Console.WriteLine("\nPresione cualquier tecla para finalizar las pruebas...");
            Console.ReadKey();
        }
    }
    internal class PruebasETL
    {

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