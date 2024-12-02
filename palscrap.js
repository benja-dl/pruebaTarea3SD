const puppeteer = require('puppeteer');
const { Kafka } = require('kafkajs');

// Desactivar la advertencia de particionador
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// Configuración de Kafka
const kafka = new Kafka({
  clientId: 'waze-scraper',
  brokers: ['172.19.0.5:9092'],
});

const producer = kafka.producer();

(async () => {
  // Iniciar Kafka
  await producer.connect();

  // Lanzar el navegador
  const browser = await puppeteer.launch({
    headless: false, // Cambiado a false para ver el navegador
    args: ['--no-sandbox', '--disable-setuid-sandbox'], // Añadido para evitar problemas con
  });

  // Abrir una nueva página
  const page = await browser.newPage();

  // Intentar navegar a Waze con manejo de errores
  try {
    await page.goto('https://www.waze.com', { waitUntil: 'domcontentloaded', timeout: 60000 });
    console.log('Página cargada correctamente');
  } catch (error) {
    console.error('Error de navegación:', error);
    await browser.close();
    return;
  }

  // Interceptar solicitudes de la red
  await page.setRequestInterception(true);
  page.on('request', (request) => {
    if (request.url().includes('web-events')) {
      console.log('Solicitud interceptada:', request.url());
      request.continue();
    } else {
      request.abort();
    }
  });

  // Escuchar las respuestas y capturar los datos
  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('web-events')) {
      try {
        const jsonResponse = await response.json();
        console.log('Datos de tráfico capturados:', jsonResponse);

        // Verificar que los datos sean válidos antes de enviarlos
        if (jsonResponse) {
          // Enviar los datos al tema de Kafka
          await producer.send({
            topic: 'waze_traffic',
            messages: [
              {
                value: JSON.stringify(jsonResponse),
              },
            ],
          });

          console.log('Datos enviados a Kafka');
        } else {
          console.log('Respuesta vacía o no válida');
        }
      } catch (error) {
        console.error('Error al procesar la respuesta:', error);
      }
    }
  });

  // Esperar hasta que el selector o evento esté disponible (alternativa a waitForTimeout)
  await page.waitForSelector('body'); // Esperamos que el cuerpo de la página esté disponible

  // Cerrar el navegador
  await browser.close();

  // Desconectar de Kafka
  await producer.disconnect();
})();
